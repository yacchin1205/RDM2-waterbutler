import http
import tempfile
import requests
from io import BytesIO
from lxml import etree
import logging

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.utils import AsyncIterator

from waterbutler.providers.weko import settings
from waterbutler.providers.weko.metadata import WEKORevision
from waterbutler.providers.weko.metadata import WEKOItemMetadata
from waterbutler.providers.weko.metadata import WEKOIndexMetadata
from waterbutler.providers.weko import client

logger = logging.getLogger('waterbutler.providers.weko')

class WEKOProvider(provider.BaseProvider):
    """Provider for WEKO"""

    NAME = 'weko'
    connection = None

    def __init__(self, auth, credentials, settings):
        """
        :param dict auth: Not used
        :param dict credentials: Contains `token`
        :param dict settings: Contains `host`, `doi`, `id`, and `name` of a dataset. Hosts::

            - 'demo.dataverse.org': Harvard Demo Server
            - 'dataverse.harvard.edu': Dataverse Production Server **(NO TEST DATA)**
            - Other
        """
        super().__init__(auth, credentials, settings)
        self.BASE_URL = 'http://104.198.102.120/weko-oauth/htdocs/weko/sword/'

        self.token = self.credentials['token']
        self.doi = self.settings['doi']
        self._id = self.settings['id']
        self.name = self.settings['name']
        self.connection = client.connect_or_error(self.BASE_URL, self.token)

        self._metadata_cache = {}

    def build_url(self, path, *segments, **query):
        # Need to split up the dataverse subpaths and push them into segments
        return super().build_url(*(tuple(path.split('/')) + segments), **query)

    def can_duplicate_names(self):
        return False

    async def validate_v1_path(self, path, **kwargs):
        return await self.validate_path(path, **kwargs)

    async def validate_path(self, path, revision=None, **kwargs):
        """Ensure path is in configured dataset

        :param str path: The path to a file
        :param list metadata: List of file metadata from _get_data
        """
        wbpath = WaterButlerPath(path)
        wbpath.revision = revision
        return wbpath

    async def revalidate_path(self, base, path, folder=False, revision=None):
        path = path.strip('/')

        wbpath = None
        for item in (await self._maybe_fetch_metadata(version=revision)):
            if path == item.name:
                # Dataverse cant have folders
                wbpath = base.child(item.name, _id=item.extra['fileId'], folder=False)
        wbpath = wbpath or base.child(path, _id=None, folder=False)

        wbpath.revision = revision or base.revision
        return wbpath


    async def download(self, path, revision=None, range=None, **kwargs):
        """Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from Dataverse is not 200

        :param str path: Path to the file you want to download
        :param str revision: Used to verify if file is in selected dataset

            - 'latest' to check draft files
            - 'latest-published' to check published files
            - None to check all data
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        resp = await self.make_request(
            'GET',
            self.build_url(settings.DOWN_BASE_URL, path.identifier, key=self.token),
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, **kwargs):
        """Zips the given stream then uploads to Dataverse.
        This will delete existing draft files with the same name.

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to Dataverse
        :param str path: The filename prepended with '/'

        :rtype: dict, bool
        """

        stream = streams.ZipStreamReader(AsyncIterator([(path.name, stream)]))

        # Write stream to disk (Necessary to find zip file size)
        f = tempfile.TemporaryFile()
        chunk = await stream.read()
        while chunk:
            f.write(chunk)
            chunk = await stream.read()
        stream = streams.FileStreamReader(f)

        dv_headers = {
            "Content-Disposition": "filename=temp.zip",
            "Content-Type": "application/zip",
            "Packaging": "http://purl.org/net/sword/package/SimpleZip",
            "Content-Length": str(stream.size),
        }

        # Delete old file if it exists
        if path.identifier:
            await self.delete(path)

        resp = await self.make_request(
            'POST',
            self.build_url(settings.EDIT_MEDIA_BASE_URL, 'study', self.doi),
            headers=dv_headers,
            auth=(self.token, ),
            data=stream,
            expects=(201, ),
            throws=exceptions.UploadError
        )
        await resp.release()

        # Find appropriate version of file
        metadata = await self._get_data('latest')
        files = metadata if isinstance(metadata, list) else []
        file_metadata = next(file for file in files if file.name == path.name)

        return file_metadata, path.identifier is None

    async def delete(self, path, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        """
        # Can only delete files in draft
        path = await self.validate_path('/' + path.identifier, version='latest', throw=True)

        resp = await self.make_request(
            'DELETE',
            self.build_url(settings.EDIT_MEDIA_BASE_URL, 'file', path.identifier),
            auth=(self.token, ),
            expects=(204, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def metadata(self, path, version=None, **kwargs):
        """
        :param str version:

            - 'latest' for draft files
            - 'latest-published' for published files
            - None for all data
        """
        version = version or path.revision
        logger.info('Path: {path}, href={href}'.format(path=path.path, href=self.doi))
        indices = client.get_all_indices(self.connection, self.doi)

        if path.is_root:
            return [WEKOIndexMetadata(index, indices)
                    for index in indices if index.nested == 0]
        if path.is_dir:
            parent = path.path.split('/')[-2]
            index = [index
                     for index in indices if str(index.identifier) == parent][0]

            ritems = [WEKOItemMetadata(item, index)
                      for item in client.get_items(self.connection, index)]

            rindices = [WEKOIndexMetadata(index, indices)
                        for index in indices if str(index.parentIdentifier) == parent]
            return rindices + ritems
 
        raise exceptions.MetadataError('unsupported', code=404)

    async def revisions(self, path, **kwargs):
        """Get past versions of the request file. Orders versions based on
        `_get_all_data()`

        :param str path: The path to a key
        :rtype list:
        """

        metadata = await self._get_data()
        return [
            WEKORevision(item.extra['datasetVersion'])
            for item in metadata if item.extra['fileId'] == path.identifier
        ]
