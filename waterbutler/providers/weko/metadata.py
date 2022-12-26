import os
import re
import hashlib
import zipfile
from datetime import datetime
from waterbutler.core import metadata

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
        }


class WEKOItemMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    index = None

    def __init__(self, raw, index):
        super().__init__(raw)
        self.index = index

    @property
    def file_id(self):
        return _get_item_file_id(self.raw)

    @property
    def name(self):
        return self.raw.title

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
        }
        r.update(self.raw.extra)
        return r
