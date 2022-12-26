import csv
import json
import io
import os
import tempfile
import logging
import hashlib
from zipfile import ZipFile
import shutil
from urllib.parse import urlparse, urlunparse
from lxml import etree

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core import utils
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.weko.metadata import (
    ITEM_PREFIX,
    split_path,
    WEKOFileMetadata,
    WEKOItemMetadata,
    WEKOIndexMetadata,
    WEKODraftFileMetadata,
)
from waterbutler.providers.weko.client import Client
from waterbutler.providers.weko import settings

logger = logging.getLogger(__name__)
METADATA_JSON_SUFFIX = '-metadata.json'


class WEKOProvider(provider.BaseProvider):
    """Provider for WEKO"""

    NAME = 'weko'
    connection = None

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        :param dict auth: Not used
        :param dict credentials: Contains `token`
        :param dict settings: Contains `url`, `index_id` and `index_title` of a repository.
        """
        super().__init__(auth, credentials, settings, **kwargs)
        self.nid = self.settings['nid']
        self.BASE_URL = self.settings['url']

        self.user_id = self.credentials['user_id']
        self.index_id = self.settings['index_id']
        self.index_title = self.settings['index_title']
        self.default_storage_credentials = credentials.get('default_storage', None)
        self.default_storage_settings = settings.get('default_storage', None)
        if 'token' in self.credentials:
            self.client = Client(self.BASE_URL,
                                 token=self.credentials['token'])
        else:
            self.client = Client(self.BASE_URL,
                                 username=self.user_id,
                                 password=self.credentials['password'])

    def _resolve_target_index(self, index_path):
        if index_path is None:
            return str(self.index_id)
        else:
            return index_path.split('/')[-2][len(ITEM_PREFIX):]

    def make_default_provider(self):
        if not getattr(self, '_default_provider', None):
            logger.info('make_provider: osfstorage - begin')
            self._default_provider = utils.make_provider(
                'osfstorage',
                self.auth,
                self.default_storage_credentials,
                self.default_storage_settings,
                is_celery_task=self.is_celery_task,
            )
            logger.info('make_provider: osfstorage - end')
        return self._default_provider

    def path_from_metadata(self, parent_path, metadata):
        return parent_path.child(metadata.materialized_name,
                                 _id=metadata.path.strip('/'),
                                 folder=metadata.is_folder)

    def build_url(self, path, *segments, **query):
        return super().build_url(*(tuple(path.split('/')) + segments), **query)

    def can_duplicate_names(self):
        return False

    async def validate_v1_path(self, path, **kwargs):
        return await self.validate_path(path, **kwargs)

    async def validate_path(self, path, revision=None, **kwargs):
        """Ensure path is in configured index

        :param str path: The path to a file
        :param list metadata: List of file metadata from _get_data
        """
        return WaterButlerPath(path)

    async def upload(self, stream, path, **kwargs):
        try:
            index_path, item_id, draft_path = split_path(path.path)
            if item_id is not None:
                raise exceptions.MetadataError('Cannot upload files to the item', code=404)
            index_id = self._resolve_target_index(index_path)
            default_provider, index_folder = await self.get_index_folder(index_id, creates=True)

            logger.debug(f'Draft folder: {index_folder}')
            draft_path = await default_provider.validate_path(
                index_folder.path + draft_path
            )

            stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
            stream.add_writer('sha1', streams.HashStreamWriter(hashlib.sha1))
            stream.add_writer('sha256', streams.HashStreamWriter(hashlib.sha256))
            stream.add_writer('sha512', streams.HashStreamWriter(hashlib.sha512))

            return await default_provider.upload(
                stream, draft_path, **kwargs
            )
        except:
            logger.exception('TEST')
            raise exceptions.MetadataError('unsupported', code=404)

    async def delete(self, path, confirm_delete=0, **kwargs):
        raise exceptions.MetadataError('Unsupported operation', code=404)

    async def download(self, path, revision=None, range=None, **kwargs):
        index_path, item_id, file_path = split_path(path.path)
        parent = self._resolve_target_index(index_path)
        index = self.client.get_index_by_id(parent)
        item = index.get_item_by_id(item_id)
        files = [f for f in item.files if f.filename == file_path]
        if len(files) == 0:
            raise exceptions.MetadataError('File not found', code=404)
        file = files[0]
        resp = await self.make_request(
            'GET',
            file.download_url,
            range=range,
            headers=self.client.request_headers(),
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        return streams.ResponseStreamReader(resp)

    async def metadata(self, path, version=None, **kwargs):
        """
        :param str version:

            - 'latest' for draft files
            - 'latest-published' for published files
            - None for all data
        """
        index_path, item_id, draft_path = split_path(path.path)

        if path.is_root:
            parent = str(self.index_id)
        elif path.is_dir:
            parent = self._resolve_target_index(index_path)
        elif len(draft_path) > 0:
            parent = self._resolve_target_index(index_path)
        else:
            raise exceptions.MetadataError('unsupported', code=404)

        try:
            index = self.client.get_index_by_id(parent)
        except ValueError:
            raise exceptions.MetadataError('Index not found', code=404)
        if len(draft_path) > 0:
            default_provider, index_folder = await self.get_index_folder(parent)
            if index_folder is None:
                raise exceptions.MetadataError('Unexpected path', code=404)
            draft_file_path = await default_provider.validate_path(index_folder.path + draft_path)
            file_metadata = await default_provider.metadata(draft_file_path)
            return WEKODraftFileMetadata(file_metadata, index)
        elif item_id is not None:
            item = index.get_item_by_id(item_id)
            ritems = [WEKOFileMetadata(f, item, index) for f in item.files]
            return ritems
        else:
            # WEKO index
            ritems = [WEKOItemMetadata(item, index) for item in index.get_items()]
            rindices = [WEKOIndexMetadata(i) for i in index.children]
            default_provider, index_folder = await self.get_index_folder(parent)
            rdrafts = []
            if index_folder is not None:
                index_folder_path = await default_provider.validate_path(index_folder.path)
                index_folder_metadata = await default_provider.metadata(index_folder_path)
                for f in index_folder_metadata:
                    rdrafts.append(WEKODraftFileMetadata(f, index))
            return rindices + ritems + rdrafts

    async def revisions(self, path, **kwargs):
        """Get past versions of the request file.

        :param str path: The path to a key
        :rtype list:
        """

        return []

    async def get_draft_folder(self, creates=False):
        default_provider = self.make_default_provider()
        root_folder_path = await default_provider.validate_path('/')
        root_folder_metadata = await default_provider.metadata(
            root_folder_path
        )
        draft_folders = [child
                         for child in root_folder_metadata
                         if child.name == f'.{self.NAME}']
        if len(draft_folders) > 0:
            return default_provider, draft_folders[0]
        if not creates:
            return default_provider, None
        # Create draft folder
        draft_folder_path = await default_provider.validate_path(f'/.{self.NAME}/')
        folder = await default_provider.create_folder(draft_folder_path)
        return default_provider, folder

    async def get_index_folder(self, index_id, creates=False):
        default_provider, draft_folder = await self.get_draft_folder(creates=creates)
        if draft_folder is None:
            return default_provider, None
        draft_folder_path = await default_provider.validate_path(draft_folder.path)
        draft_folder_metadata = await default_provider.metadata(
            draft_folder_path
        )
        index_folders = [child
                         for child in draft_folder_metadata
                         if child.name == index_id]
        if len(index_folders) > 0:
            return default_provider, index_folders[0]
        if not creates:
            return default_provider, None
        # Create index folder
        index_folder_path = await default_provider.validate_path(f'{draft_folder.path}{index_id}/')
        index_folder = await default_provider.create_folder(index_folder_path)
        return default_provider, index_folder