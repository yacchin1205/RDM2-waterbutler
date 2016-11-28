import os
import hashlib
import functools
import tempfile
from urllib import parse

import xmltodict

import xml.sax.saxutils

from azure.storage.blob import BlockBlobService
from azure.common import AzureHttpError

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.azureblobstorage import settings
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFileMetadata
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFolderMetadata
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFileMetadataHeaders


class AzureBlobStorageProvider(provider.BaseProvider):
    """Provider for Azure Blob Storage cloud storage service.
    """
    NAME = 'azureblobstorage'

    def __init__(self, auth, credentials, settings):
        """
        :param dict auth: Not used
        :param dict credentials: Dict containing `username`, `password` and `tenant_name`
        :param dict settings: Dict containing `container`
        """
        super().__init__(auth, credentials, settings)

        self.connection = BlockBlobService(account_name=credentials['account_name'],
                                           account_key=credentials['account_key'])

        self.container = settings['container']

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path)

        implicit_folder = path.endswith('/')

        assert path.startswith('/')
        if implicit_folder:
            objects = self.connection.list_blobs(self.container)
            if len(list(filter(lambda o: o.name.startswith(path[1:]),
                               objects))) == 0:
                raise exceptions.NotFoundError(str(path))
        else:
            try:
                self.connection.get_blob_properties(self.container, path[1:])
            except azureblobstorage_exceptions.ClientException:
                raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        # Not supported
        return False

    def can_intra_move(self, dest_provider, path=None):
        # Not supported
        return False

    async def intra_copy(self, dest_provider, source_path, dest_path):
        # Not supported
        raise NotImplementedError()

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
        content = self.connection.get_blob_to_bytes(self.container,
                                                    path.path)
        stream = streams.StringStream(content.content)
        stream.content_type = content.properties.content_settings.content_type
        return stream

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to Azure Blob Storage

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to Azure Blob Storage
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
            assert not path.path.startswith('/')
            self.connection.delete_object(self.container, path.path)
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_folder(self, path, **kwargs):
        raise NotImplementedError()

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

        self.connection.create_blob_from_text(self.container, path.path + '.azureblobstoragekeep', '')
        return AzureBlobStorageFolderMetadata({'prefix': path.path})

    async def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None
        assert not path.path.startswith('/')
        try:
            resp = self.connection.get_blob_properties(self.container, path.path)
            return AzureBlobStorageFileMetadataHeaders(path.path, resp)
        except AzureHttpError as e:
            raise exceptions.MetadataError(str(e), code=e.status_code)

    async def _metadata_folder(self, path):
        objects = self.connection.list_blobs(self.container)
        objects = list(map(lambda o: (o.name[len(path.path):], o),
                           filter(lambda o: o.name.startswith(path.path),
                                  objects)))
        if len(objects) == 0:
            raise exceptions.MetadataError('Not found', code=404)

        contents = list(filter(lambda o: '/' not in o[0], objects))
        prefixes = sorted(set(map(lambda o: o[0][:o[0].index('/') + 1],
                                  filter(lambda o: '/' in o[0], objects))))

        items = [
            AzureBlobStorageFolderMetadata({'prefix': item})
            for item in prefixes
        ]

        for content_path, content in contents:
            if content_path == path.path:
                continue

            items.append(AzureBlobStorageFileMetadata(content))

        return items
