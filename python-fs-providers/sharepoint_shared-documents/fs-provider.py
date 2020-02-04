from dataiku.fsprovider import FSProvider

import os, shutil, requests, sharepy, logging

from datetime import datetime
from sharepoint_client import SharePointSession, SharePointClient
import sharepoint_client
from dss_constants import *
from sharepoint_constants import *

try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='sharepoint plugin %(levelname)s - %(message)s')

# based on https://docs.microsoft.com/fr-fr/sharepoint/dev/sp-add-ins/working-with-folders-and-files-with-rest

class SharePointFSProvider(FSProvider):
    def __init__(self, root, config, plugin_config):
        """
        :param root: the root path for this provider
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        if len(root) > 0 and root[0] == '/':
            root = root[1:]
        self.root = root
        self.provider_root = "/"
        logger.info('init:root={}'.format(self.root))

        self.client = SharePointClient(config)

    # util methods
    def get_rel_path(self, path):
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        return path
    def get_lnt_path(self, path):
        if len(path) == 0 or path == '/':
            return '/'
        elts = path.split('/')
        elts = [e for e in elts if len(e) > 0]
        return '/' + '/'.join(elts)
    def get_full_path(self, path):
        path_elts = [self.provider_root, self.get_rel_path(self.root), self.get_rel_path(path)]
        path_elts = [e for e in path_elts if len(e) > 0]
        return os.path.join(*path_elts)

    def close(self):
        logger.info('close')

    def stat(self, path):
        full_path = self.get_lnt_path(self.get_full_path(path))
        logger.info('stat:path="{}", full_path="{}"'.format(path, full_path))
        files = self.client.get_files(full_path)
        folders = self.client.get_folders(full_path)

        if self.has_sharepoint_items(files) or self.has_sharepoint_items(folders):
            return {
                DSS_PATH : self.get_lnt_path(path),
                DSS_SIZE : 0,
                DSS_LAST_MODIFIED : int(0) * 1000,
                DSS_IS_DIRECTORY : True
            }

        path_to_item, item_name = os.path.split(full_path)
        files = self.client.get_files(path_to_item)
        folders = self.client.get_folders(path_to_item)

        file = self.extract_item_from(item_name, files)
        folder = self.extract_item_from(item_name, folders)

        if folder is not None:
            return {
                DSS_PATH : self.get_lnt_path(path),
                DSS_SIZE : 0,
                DSS_LAST_MODIFIED : self.get_last_modified(folder),
                DSS_IS_DIRECTORY : True
            }
        if file is not None:
            return {
                DSS_PATH : self.get_lnt_path(path),
                DSS_SIZE : self.get_size(file),
                DSS_LAST_MODIFIED : self.get_last_modified(file),
                DSS_IS_DIRECTORY : False
            }
        return None

    def extract_item_from(self, item_name, items):
        for item in self.loop_sharepoint_items(items):
            if "Name" in item and item['Name'] == item_name:
                return item
        return None

    def set_last_modified(self, path, last_modified):
        full_path = self.get_full_path(path)
        os.utime(full_path, (os.path.getatime(full_path), last_modified / 1000))
        return True

    def browse(self, path):
        path = self.get_rel_path(path)
        full_path = self.get_lnt_path(self.get_full_path(path))
        logger.info('browse:path="{}", full_path="{}"'.format(path, full_path))

        folders = self.client.get_folders(full_path)
        files = self.client.get_files(full_path)
        children = []

        for file in self.loop_sharepoint_items(files):
            children.append({
                DSS_FULL_PATH: self.get_lnt_path(os.path.join(path, self.get_name(file))),
                DSS_EXISTS: True,
                DSS_DIRECTORY: False,
                DSS_SIZE: self.get_size(file),
                DSS_LAST_MODIFIED : self.get_last_modified(file)
            })
        for folder in self.loop_sharepoint_items(folders):
            children.append({
                DSS_FULL_PATH : self.get_lnt_path(os.path.join(path, self.get_name(folder))),
                DSS_EXISTS : True,
                DSS_DIRECTORY : True,
                DSS_SIZE : 0,
                DSS_LAST_MODIFIED : self.get_last_modified(folder)
            })

        if len(children) > 0:
            return {
                DSS_FULL_PATH : self.get_lnt_path(path),
                DSS_EXISTS : True,
                DSS_DIRECTORY : True,
                DSS_CHILDREN : children
            }
        path_to_file, file_name = os.path.split(full_path)

        files = self.client.get_files(path_to_file)

        for file in self.loop_sharepoint_items(files):
            if self.get_name(file) == file_name:
                return {
                    DSS_FULL_PATH : self.get_lnt_path(path),
                    DSS_EXISTS : True, DSS_SIZE : self.get_size(file),
                    DSS_LAST_MODIFIED:self.get_last_modified(file),
                    DSS_DIRECTORY : False
                }

        parent_path, item_name = os.path.split(full_path)
        folders = self.client.get_folders(parent_path)
        folder = self.extract_item_from(item_name, folders)
        if folder is None:
            ret = {DSS_FULL_PATH : None, DSS_EXISTS : False}
        else:
            ret = {DSS_FULL_PATH : self.get_lnt_path(path), DSS_EXISTS : True, DSS_SIZE:0}
        return ret

    def loop_sharepoint_items(self, items):
        if SHAREPOINT_RESULTS_CONTAINER_V2 not in items or SHAREPOINT_RESULTS not in items[SHAREPOINT_RESULTS_CONTAINER_V2]:
            yield
        for item in items[SHAREPOINT_RESULTS_CONTAINER_V2][SHAREPOINT_RESULTS]:
            yield item

    def has_sharepoint_items(self, items):
        if SHAREPOINT_RESULTS_CONTAINER_V2 not in items or SHAREPOINT_RESULTS not in items[SHAREPOINT_RESULTS_CONTAINER_V2]:
            return False
        if len(items[SHAREPOINT_RESULTS_CONTAINER_V2][SHAREPOINT_RESULTS]) > 0:
            return True
        else:
            return False

    def get_last_modified(self, item):
        if SHAREPOINT_TIME_LAST_MODIFIED in item:
            return int(self.format_date(item[SHAREPOINT_TIME_LAST_MODIFIED]))

    def format_date(self, date):
        if date is not None:
            utc_time = datetime.strptime(date, SHAREPOINT_TIME_FORMAT)
            epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
            return int(epoch_time) * 1000
        else:
            return None

    def generate_header(self, content_type=None):
        header = {
            'content-Type': 'application/x-www-form-urlencoded',
            'authorization': 'bearer ' + self.access_token
            }
        if content_type is not None:
            header['content-Type'] = content_type
        return header

    def enumerate(self, path, first_non_empty):
        path = self.get_rel_path(path)
        full_path = self.get_lnt_path(self.get_full_path(path))
        path_to_item, item_name = os.path.split(full_path)
        ret = self.list_recursive(path, full_path, first_non_empty)
        return ret

    def get_size(self, item):
        if SHAREPOINT_LENGTH in item:
            return int(item[SHAREPOINT_LENGTH])
        else:
            return 0

    def list_recursive(self, path, full_path, first_non_empty):
        paths = []
        folders = self.client.get_folders(full_path)
        for folder in self.loop_sharepoint_items(folders):
            paths.extend(
                self.list_recursive(
                    self.get_lnt_path(os.path.join(path, self.get_name(folder))),
                    self.get_lnt_path(os.path.join(full_path, self.get_name(folder))),
                    first_non_empty
                )
            )
        files = self.client.get_files(full_path)
        for file in self.loop_sharepoint_items(files):
            paths.append({
                DSS_PATH : self.get_lnt_path(os.path.join(path, self.get_name(file))),
                DSS_LAST_MODIFIED : self.get_last_modified(file),
                DSS_SIZE : self.get_size(file)
            })
            if first_non_empty:
                return paths
        return paths

    def delete_recursive(self, path):
        full_path = self.get_full_path(path)
        logger.info('delete_recursive:path={},fullpath={}'.format(path, full_path))
        self.assert_path_is_not_root(full_path)
        path_to_item, item_name = os.path.split(full_path)
        files = self.client.get_files(path_to_item)
        folders = self.client.get_folders(path_to_item)
        file = self.extract_item_from(item_name, files)
        folder = self.extract_item_from(item_name, folders)

        if file is not None and folder is not None:
            raise Exception("Ambiguous naming with file / folder {}".format(item_name))

        if file is not None:
            self.client.delete_file(self.get_lnt_path(full_path))
            return 1

        if folder is not None:
            self.client.delete_folder(self.get_lnt_path(full_path))
            return 1

        return 0

    def get_name(self, item):
        if "Name" in item:
            return item["Name"]
        else:
            return None

    def move(self, from_path, to_path):
        full_from_path = self.get_full_path(from_path)
        full_to_path = self.get_full_path(to_path)
        logger.info('move:from={},to={}'.format(full_from_path, full_to_path))

        response = self.client.move_file(full_from_path, full_to_path)
        return SHAREPOINT_RESULTS_CONTAINER_V2 in response and "MoveTo" in response[SHAREPOINT_RESULTS_CONTAINER_V2]

    def read(self, path, stream, limit):
        full_path = self.get_full_path(path)
        logger.info('read:full_path={}'.format(full_path))

        response = self.client.get_file_content(full_path)
        bio = BytesIO(response.content)
        shutil.copyfileobj(bio, stream)

    def write(self, path, stream):
        full_path = self.get_full_path(path)
        logger.info('write:path="{}", full_path="{}"'.format(path, full_path))
        bio = BytesIO()
        shutil.copyfileobj(stream, bio)
        bio.seek(0)
        data = bio.read()
        self.create_path(full_path)
        response = self.client.write_file_content(full_path, data)
        logger.info("write:response={}".format(response))

    def create_path(self, file_full_path):
        full_path, filename = os.path.split(file_full_path)
        tokens = full_path.split("/")
        path = ""
        for token in tokens:
            path = self.get_lnt_path(path + "/" + token)
            self.client.create_folder(path)

    def assert_path_is_not_root(self, path):
        if path is None:
            raise Exception("Cannot delete root path")
        path = self.get_rel_path(path)
        if path == "" or path == "/":
            raise Exception("Cannot delete root path")
