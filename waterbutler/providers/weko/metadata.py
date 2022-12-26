import os
import re
from datetime import datetime
from waterbutler.core import metadata

from .schema import to_metadata

ITEM_PREFIX = 'weko:'


def _get_item_file_id(item):
    return 'item{}'.format(item.identifier)

def _split_folder_path(path):
    m = re.match(r'^(.+\/)' + ITEM_PREFIX + r'item([0-9]+)\/$', path)
    if not m:
        return path, None
    return m.group(1), m.group(2)

def split_path(path):
    assert not path.startswith('/')
    if len(path) == 0:
        return (None, None, '')
    components = path.split('/')
    drafti = [i for i, c in enumerate(components)
                if len(c) > 0 and not c.startswith(ITEM_PREFIX)]
    if len(drafti) == 0:
        index_path, item_id = _split_folder_path(path)
        return (index_path, item_id, '')
    indices = components[:drafti[0]]
    if len(indices) == 0:
        return (None, None, path)
    index_path, item_id = _split_folder_path('{}/'.format('/'.join(indices)))
    assert path.startswith(index_path)
    return (index_path, item_id, '/'.join(components[drafti[0]:]))

def get_files(directory, relative=''):
    files = []
    for f in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, f)):
            files.append(os.path.join(relative, f) if len(relative) > 0 else f)
        elif os.path.isdir(os.path.join(directory, f)):
            for child in get_files(os.path.join(directory, f),
                                   os.path.join(relative, f)
                                   if len(relative) > 0 else f):
                files.append(child)
    return files


class BaseWEKOMetadata(metadata.BaseMetadata):
    @property
    def provider(self):
        return 'weko'

    @property
    def created_utc(self):
        return None


class WEKOFileMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    index = None
    item = None

    def __init__(self, file, item, index):
        super().__init__(file)
        self.index = index
        self.item = item

    @property
    def file_id(self):
        return self.raw.filename

    @property
    def name(self):
        return self.raw.filename

    @property
    def content_type(self):
        return self.raw.format

    @property
    def materialized_name(self):
        return self.raw.filename

    @property
    def path(self):
        target = self.index
        path = ITEM_PREFIX + target.identifier + '/'
        while target.parent is not None:
            target = target.parent
            path = ITEM_PREFIX + target.identifier + '/' + path
        return '/' + path + ITEM_PREFIX + _get_item_file_id(self.item) + '/' + self.name

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.raw.version_id

    @property
    def extra(self):
        return {
            'weko': 'file',
            'itemId': _get_item_file_id(self.item),
            'metadata': None,
        }


class WEKOItemMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    index = None

    def __init__(self, raw, index, provider_name, metadata_schema_id):
        super().__init__(raw)
        self.index = index
        self.provider_name = provider_name
        self.metadata_schema_id = metadata_schema_id

    @property
    def file_id(self):
        return _get_item_file_id(self.raw)

    @property
    def name(self):
        v = self.raw.title
        if isinstance(v, str):
            return v
        return v[0]

    @property
    def content_type(self):
        return None

    @property
    def materialized_name(self):
        return ITEM_PREFIX + self.file_id

    @property
    def path(self):
        target = self.index
        path = ITEM_PREFIX + target.identifier + '/'
        while target.parent is not None:
            target = target.parent
            path = ITEM_PREFIX + target.identifier + '/' + path
        return '/' + path + ITEM_PREFIX + self.file_id + '/'

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.file_id

    @property
    def extra(self):
        return {
            'weko': 'item',
            'fileId': self.file_id,
            'metadata': self._to_metadata(),
        }

    def _to_metadata(self):
        if self.metadata_schema_id is None:
            return None
        return {
            'folder': False,
            'generated': False,
            'path': self.provider_name + self.path,
            'items': [
                {
                    'active': True,
                    'data': to_metadata(self.metadata_schema_id, self.raw),
                    'schema': self.metadata_schema_id,
                    'readonly': True,
                }
            ],
        }


class WEKOIndexMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    def __init__(self, raw):
        super().__init__(raw)

    @property
    def name(self):
        return self.raw.title

    @property
    def materialized_name(self):
        return ITEM_PREFIX + self.raw.identifier

    @property
    def path(self):
        target = self.raw
        path = ITEM_PREFIX + target.identifier + '/'
        while target.parent is not None:
            target = target.parent
            path = ITEM_PREFIX + target.identifier + '/' + path
        return '/' + path

    @property
    def extra(self):
        return {
            'weko': 'index',
            'indexId': self.raw.identifier,
            'metadata': None,
        }

class WEKODraftFileMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    index = None
    file = None

    def __init__(self, file, index):
        super().__init__(file)
        self.index = index

    @property
    def name(self):
        return self.raw.name

    @property
    def content_type(self):
        return self.raw.content_type

    @property
    def materialized_name(self):
        return ITEM_PREFIX + self.raw.name

    @property
    def path(self):
        target = self.index
        path = ITEM_PREFIX + target.identifier + '/'
        while target.parent is not None:
            target = target.parent
            path = ITEM_PREFIX + target.identifier + '/' + path
        return '/' + path + self.raw.name

    @property
    def path(self):
        target = self.index
        path = ITEM_PREFIX + target.identifier + '/'
        while target.parent is not None:
            target = target.parent
            path = ITEM_PREFIX + target.identifier + '/' + path
        return '/' + path + self.raw.name

    @property
    def size(self):
        return self.raw.size

    @property
    def modified(self):
        return self.raw.modified

    @property
    def etag(self):
        return self.raw.etag

    @property
    def extra(self):
        r = {
            'weko': 'draft',
            'index': self.index.identifier,
            'source': {
                'provider': self.raw.provider,
                'path': self.raw.path,
                'materialized_path': self.raw.materialized_path,
            },
        }
        r.update(self.raw.extra)
        return r
