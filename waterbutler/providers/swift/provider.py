import os
import hashlib
import functools
import tempfile
from urllib import parse

import xmltodict

import xml.sax.saxutils

from boto import config as boto_config
from boto.compat import BytesIO
from boto.utils import compute_md5
from boto.auth import get_auth_handler
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat

from swiftclient import Connection
from swiftclient import exceptions as swift_exceptions

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.swift import settings
from waterbutler.providers.swift.metadata import SwiftFileMetadata
from waterbutler.providers.swift.metadata import SwiftFolderMetadata
from waterbutler.providers.swift.metadata import SwiftFolderKeyMetadata
from waterbutler.providers.swift.metadata import SwiftFileMetadataHeaders


class SwiftProvider(provider.BaseProvider):
    """Provider for NII Swift cloud storage service.
    """
    NAME = 'swift'

    def __init__(self, auth, credentials, settings):
        """
        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings)

        self.connection = Connection(auth_version='2',
                                     authurl='http://inter-auth.ecloud.nii.ac.jp:5000/v2.0/',
                                     user=credentials['access_key'],
                                     key=credentials['secret_key'],
                                     tenant_name='yamaji')

        self.container = settings['bucket']
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.region = None

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path)

        implicit_folder = path.endswith('/')

        assert path.startswith('/')
        if implicit_folder:
            resp, objects = self.connection.get_container(self.container)
            if len(list(filter(lambda o: o['name'].startswith(path[1:]),
                               objects))) == 0:
                raise exceptions.NotFoundError(str(path))
        else:
            try:
                resp = self.connection.head_object(self.container, path[1:])
            except swift_exceptions.ClientException:
                raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    def can_intra_move(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    async def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one S3 bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        exists = await dest_provider.exists(dest_path)

        dest_key = dest_provider.bucket.new_key(dest_path.path)

        # ensure no left slash when joining paths
        source_path = '/' + os.path.join(self.settings['bucket'], source_path.path)
        headers = {'x-amz-copy-source': parse.quote(source_path)}
        url = functools.partial(
            dest_key.generate_url,
            settings.TEMP_URL_SECS,
            'PUT',
            headers=headers,
        )
        resp = await self.make_request(
            'PUT', url,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(200, ),
            throws=exceptions.IntraCopyError,
        )
        await resp.release()
        return (await dest_provider.metadata(dest_path)), not exists

    async def download(self, path, accept_url=False, version=None, range=None, **kwargs):
        """
        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        assert not path.path.startswith('/')
        resp, content = self.connection.get_object(self.container,
                                                   path.path)
        stream = streams.StringStream(content)
        stream.content_type = resp['content-type']
        return stream

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to NII Swift

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to Swift
        :param str path: The full path of the key to upload to/into

        :rtype: dict, bool
        """

        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        assert not path.path.startswith('/')

        with tempfile.TemporaryFile() as f:
            while True:
                chunk = await stream.read(100)
                if not chunk:
                    break
                f.write(chunk)
            f.seek(0)
            etag = self.connection.put_object(self.container, path.path, f)
            assert etag == stream.writers['md5'].hexdigest

        return (await self.metadata(path, **kwargs)), not exists

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """

        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        if path.is_file:
            resp = await self.make_request(
                'DELETE',
                self.bucket.new_key(path.path).generate_url(settings.TEMP_URL_SECS, 'DELETE'),
                expects=(200, 204, ),
                throws=exceptions.DeleteError,
            )
            await resp.release()
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_folder(self, path, **kwargs):
        """Query for recursive contents of folder and delete in batches of 1000

        Called from: func: delete if not path.is_file

        Calls: func: self._check_region
               func: self.make_request
               func: self.bucket.generate_url

        :param *ProviderPath path: Path to be deleted

        On S3, folders are not first-class objects, but are instead inferred
        from the names of their children.  A regular DELETE request issued
        against a folder will not work unless that folder is completely empty.
        To fully delete an occupied folder, we must delete all of the comprising
        objects.  Amazon provides a bulk delete operation to simplify this.
        """

        more_to_come = True
        content_keys = []
        query_params = {'prefix': path.path}
        marker = None

        while more_to_come:
            if marker is not None:
                query_params['marker'] = marker

            resp = await self.make_request(
                'GET',
                self.bucket.generate_url(settings.TEMP_URL_SECS, 'GET', query_parameters=query_params),
                params=query_params,
                expects=(200, ),
                throws=exceptions.MetadataError,
            )

            contents = await resp.read()
            parsed = xmltodict.parse(contents, strip_whitespace=False)['ListBucketResult']
            more_to_come = parsed.get('IsTruncated') == 'true'
            contents = parsed.get('Contents', [])

            if isinstance(contents, dict):
                contents = [contents]

            content_keys.extend([content['Key'] for content in contents])
            if len(content_keys) > 0:
                marker = content_keys[-1]

        # Query against non-existant folder does not return 404
        if len(content_keys) == 0:
            raise exceptions.NotFoundError(str(path))

        while len(content_keys) > 0:
            key_batch = content_keys[:1000]
            del content_keys[:1000]

            payload = '<?xml version="1.0" encoding="UTF-8"?>'
            payload += '<Delete>'
            payload += ''.join(map(
                lambda x: '<Object><Key>{}</Key></Object>'.format(xml.sax.saxutils.escape(x)),
                key_batch
            ))
            payload += '</Delete>'
            payload = payload.encode('utf-8')
            md5 = compute_md5(BytesIO(payload))

            query_params = {'delete': ''}
            headers = {
                'Content-Length': str(len(payload)),
                'Content-MD5': md5[1],
                'Content-Type': 'text/xml',
            }

            # We depend on a customized version of boto that can make query parameters part of
            # the signature.
            url = functools.partial(
                self.bucket.generate_url,
                settings.TEMP_URL_SECS,
                'POST',
                query_parameters=query_params,
                headers=headers,
            )
            resp = await self.make_request(
                'POST',
                url,
                params=query_params,
                data=payload,
                headers=headers,
                expects=(200, 204, ),
                throws=exceptions.DeleteError,
            )
            await resp.release()

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        """
        return []

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        if path.is_dir:
            return (await self._metadata_folder(path))

        return (await self._metadata_file(path, revision=revision))

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """

        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(str(path))

        async with self.request(
            'PUT',
            functools.partial(self.bucket.new_key(path.path).generate_url, settings.TEMP_URL_SECS, 'PUT'),
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201),
            throws=exceptions.CreateFolderError
        ):
            return SwiftFolderMetadata({'Prefix': path.path})

    async def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None
        assert not path.path.startswith('/')
        try:
            resp = self.connection.head_object(self.container, path.path)
            return SwiftFileMetadataHeaders(path.path, resp)
        except swift_exceptions.ClientException as e:
            raise exceptions.MetadataError(str(e), code=e.http_status)

    async def _metadata_folder(self, path):
        resp, objects = self.connection.get_container(self.container)
        objects = list(map(lambda o: (o['name'][len(path.path):], o),
                           filter(lambda o: o['name'].startswith(path.path),
                                  objects)))
        contents = list(filter(lambda o: '/' not in o[0], objects))
        prefixes = sorted(set(map(lambda o: o[0][:o[0].index('/') + 1],
                                  filter(lambda o: '/' in o[0], objects))))

        items = [
            SwiftFolderMetadata({'prefix': item})
            for item in prefixes
        ]

        for content_path, content in contents:
            if content_path == path.path:
                continue

            items.append(SwiftFileMetadata(content))

        return items
