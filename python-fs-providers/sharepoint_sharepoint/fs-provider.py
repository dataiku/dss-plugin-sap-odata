from dataiku.fsprovider import FSProvider

import os, shutil, requests, sharepy, logging

from datetime import datetime

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
        self.provider_root = "/"#config['providerRoot']
        logger.info('init:root={}'.format(self.root))

        self.sharepoint_tenant = plugin_config.get('sharepoint_sharepy')['sharepoint_tenant']
        self.sharepoint_site = plugin_config.get('sharepoint_sharepy')['sharepoint_site']
        username = plugin_config.get('sharepoint_sharepy')['sharepoint_username']
        password = plugin_config.get('sharepoint_sharepy')['sharepoint_password']
        self.sharepoint_url = self.sharepoint_tenant + ".sharepoint.com"
        self.client = sharepy.connect(self.sharepoint_url, username=username, password=password)

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
        """
        Perform any necessary cleanup
        """
        logger.info('close')

    def stat(self, path):
        """
        Get the info about the object at the given path inside the provider's root, or None 
        if the object doesn't exist
        """
        full_path = self.get_lnt_path(self.get_full_path(path))
        logger.info('stat:path="{}", full_path="{}"'.format(path, full_path))
        files = self.get_files(full_path)
        folders = self.get_folders(full_path)

        if self.has_sharepoint_items(files) or self.has_sharepoint_items(folders):
            ret = {
                'path': self.get_lnt_path(path),
                'size':0,
                'lastModified':int(0) * 1000,
                'isDirectory':True
            }
            logger.info('stat:ret1={}'.format(ret))
            return ret

        path_to_item, item_name = os.path.split(full_path)
        files = self.get_files(path_to_item)
        folders = self.get_folders(path_to_item)

        file = self.extract_item_from(item_name, files)
        folder = self.extract_item_from(item_name, folders)

        if folder is not None:
            ret = {
                'path': self.get_lnt_path(path),
                'size':0,
                'lastModified':self.get_last_modified(folder),
                'isDirectory':True
            }
            logger.info('stat:ret2={}'.format(ret))
            return ret
        if file is not None:
            ret = {
                'path': self.get_lnt_path(path),
                'size':self.get_size(file),
                'lastModified':self.get_last_modified(file),
                'isDirectory':False
            }
            logger.info('stat:ret3={}'.format(ret))
            return ret
        logger.info('ret4=None')
        return None

    def extract_item_from(self, item_name, items):
        for item in self.loop_sharepoint_items(items):
            if "Name" in item and item['Name'] == item_name:
                return item
        return None

    def set_last_modified(self, path, last_modified):
        """
        Set the modification time on the object denoted by path. Return False if not possible
        """
        full_path = self.get_full_path(path)
        os.utime(full_path, (os.path.getatime(full_path), last_modified / 1000))
        return True

    def browse(self, path):
        """
        List the file or directory at the given path, and its children (if directory)
        """
        path = self.get_rel_path(path)
        full_path = self.get_lnt_path(self.get_full_path(path))

        logger.info('browse:path="{}", full_path="{}"'.format(path, full_path))

        folders = self.get_folders(full_path)

        files = self.get_files(full_path)

        children = []

        for file in self.loop_sharepoint_items(files):
            children.append({
                'fullPath': self.get_lnt_path(os.path.join(path, self.get_name(file))),
                'exists': True,
                'directory': False,
                'size': int(file['Length']),
                'lastModified' : self.get_last_modified(file)
            })
        for folder in self.loop_sharepoint_items(folders):
            children.append({
                'fullPath': self.get_lnt_path(os.path.join(path, self.get_name(folder))),
                'exists': True,
                'directory': True,
                'size': 0,
                'lastModified' : self.get_last_modified(folder)
            })

        if len(children) > 0:
            ret = {'fullPath' : self.get_lnt_path(path), 'exists' : True, 'directory' : True, 'children' : children}
            logger.info('browse:ret={}'.format(ret))
            return ret
        path_to_file, file_name = os.path.split(full_path)

        files = self.get_files(path_to_file)

        for file in self.loop_sharepoint_items(files):
            if self.get_name(file) == file_name:
                ret = {'fullPath' : self.get_lnt_path(path), 'exists' : True, 'size':int(file['Length']), 'lastModified':self.get_last_modified(file), 'directory' : False}
                logger.info('browse:ret={}'.format(ret))
                return ret

        parent_path, item_name = os.path.split(full_path)
        folders = self.get_folders(parent_path)
        folder = self.extract_item_from(item_name, folders)
        if folder is None:
            ret = {'fullPath' : None, 'exists' : False}
        else:
            ret = {'fullPath' : self.get_lnt_path(path), 'exists' : True, 'size':0}
        logger.info('browse:ret={}'.format(ret))
        return ret

    def loop_sharepoint_items(self, items):
        if "d" not in items or "results" not in items['d']:
            yield
        for item in items['d']['results']:
            yield item

    def has_sharepoint_items(self, items):
        if "d" not in items or "results" not in items['d']:
            return False
        if len(items['d']['results']) > 0:
            return True
        else:
            return False

    def get_folders(self, path):
        return self.client.get(self.get_sharepoint_item_url(path) + "/Folders" ).json()

    def get_files(self, path):
        return self.client.get(self.get_sharepoint_item_url(path) + "/Files" ).json()

    def get_sharepoint_item_url(self, path):
        URL_STRUCTURE = "https://{}.sharepoint.com/sites/{}/_api/Web/GetFolderByServerRelativePath(decodedurl='/sites/dssplugin/Shared%20Documents{}')"
        if path == '/':
            path = ""
        return URL_STRUCTURE.format(self.sharepoint_tenant, self.sharepoint_site, path)

    def get_last_modified(self, item):
        if 'TimeLastModified' in item:
            return int(self.format_date(item["TimeLastModified"]))
        return

    def format_date(self, date):
        if date is not None:
            utc_time = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
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
        """
        Enumerate files recursively from prefix. If first_non_empty, stop at the first non-empty file.
        
        If the prefix doesn't denote a file or folder, return None
        """
        path = self.get_rel_path(path)
        full_path = self.get_lnt_path(self.get_full_path(path))
        path_to_item, item_name = os.path.split(full_path)
        logger.info('enumerate:path="{}", full_path="{}", first_non_empty={}'.format(path, full_path, first_non_empty))

        ret = self.list_recursive(path, full_path, first_non_empty)
        logger.info('enumerate:ret={}'.format(ret))
        return ret

    def get_size(self, item):
        if "Length" in item:
            return int(item['Length'])
        else:
            return 0

    def list_recursive(self, path, full_path, first_non_empty):
        paths = []
        folders = self.get_folders(full_path)
        for folder in self.loop_sharepoint_items(folders):
            paths.extend(
                self.list_recursive(
                    self.get_lnt_path(os.path.join(path, self.get_name(folder))),
                    self.get_lnt_path(os.path.join(full_path, self.get_name(folder))),
                    first_non_empty
                )
            )
        files = self.get_files(full_path)
        for file in self.loop_sharepoint_items(files):
            paths.append({
                'path':self.get_lnt_path(os.path.join(path, self.get_name(file))),
                'lastModified':self.get_last_modified(file),
                'size':self.get_size(file)
            })
            if first_non_empty:
                return paths
        return paths

    def delete_recursive(self, path):
        """
        Delete recursively from path. Return the number of deleted files (optional)
        """
        full_path = self.get_full_path(path)
        logger.info('delete_recursive:path={},fullpath={}'.format(path, full_path))
        path_to_item, item_name = os.path.split(full_path)
        files = self.get_files(path_to_item)
        folders = self.get_folders(path_to_item)
        file = self.extract_item_from(item_name, files)
        folder = self.extract_item_from(item_name, folders)

        if file is not None and folder is not None:
            raise Exception("Ambiguous naming with file / folder {}".format(item_name))

        if file is not None:
            self.delete_file(full_path)
            return 1

        if folder is not None:
            self.delete_folder(full_path)
            return 1

        return 0

    def delete_file(self, full_path):
        FILE_DELETE_URL = "https://{}.sharepoint.com/sites/{}/_api/web/GetFileByServerRelativeUrl('/sites/dssplugin/Shared%20Documents{}')"
        headers = {
            "X-HTTP-Method":"DELETE"
        }
        response = self.client.post(
            FILE_DELETE_URL.format(
                self.sharepoint_tenant,
                self.sharepoint_site,
                self.get_lnt_path(full_path)
            ),
            headers = headers
        )

    def delete_folder(self, full_path):
        FOLDER_DELETE_URL = "https://{}.sharepoint.com/sites/{}/_api/web/GetFolderByServerRelativeUrl('/sites/dssplugin/Shared%20Documents{}')"
        headers = {
            "X-HTTP-Method":"DELETE"
        }
        response = self.client.post(
            FOLDER_DELETE_URL.format(
                self.sharepoint_tenant,
                self.sharepoint_site,
                self.get_lnt_path(full_path)
            ),
            headers = headers
        )

    def get_name(self, item):
        if "Name" in item:
            return item["Name"]
        else:
            return None

    def move(self, from_path, to_path):
        """
        Move a file or folder to a new path inside the provider's root. Return false if the moved file didn't exist
        """
        full_from_path = self.get_full_path(from_path)
        full_to_path = self.get_full_path(to_path)
        ITEM_MOVE_URL = "https://{}.sharepoint.com/sites/{}/_api/SP.MoveCopyUtil.MoveFileByPath(overwrite=@a1)?@a1=true"

        from_url = "https://{}.sharepoint.com/sites/{}/Shared Documents{}".format(
            self.sharepoint_tenant,
            self.sharepoint_site,
            self.get_lnt_path(full_from_path)
        )
        to_url = "https://{}.sharepoint.com/sites/{}/Shared Documents{}".format(
            self.sharepoint_tenant,
            self.sharepoint_site,
            self.get_lnt_path(full_to_path)
        )
        json_data = {
            "srcPath": {
                "__metadata": {
                    "type": "SP.ResourcePath"
                },
                "DecodedUrl": from_url
            },
            "destPath": {
                "__metadata": {
                    "type": "SP.ResourcePath"
                },
                "DecodedUrl": to_url
            }
        }
        response = self.client.post(ITEM_MOVE_URL.format(
                self.sharepoint_tenant,
                self.sharepoint_site
            ), 
            json = json_data
        )
        #response == {'d': {'MoveFileByPath': None}}
        return "d" in response

    def read(self, path, stream, limit):
        """
        Read the object denoted by path into the stream. Limit is an optional bound on the number of bytes to send
        """
        full_path = self.get_full_path(path)
        logger.info('read:full_path={}'.format(full_path))
        response = self.client.get(
            "https://{}.sharepoint.com/sites/{}/_api/Web/GetFileByServerRelativePath(decodedurl='/sites/dssplugin/Shared%20Documents{}')/$value".format(
                self.sharepoint_tenant,
                self.sharepoint_site,
                full_path
            )
        )
        bio = BytesIO(response.content)
        shutil.copyfileobj(bio, stream)
        # Reading lists:
        # https://{}.sharepoint.com/sites/{}/_api/Web/lists/GetByTitle('AlexTestList')/Items

    def write(self, path, stream):
        """
        Write the stream to the object denoted by path into the stream
        """
        full_path = self.get_full_path(path)
        full_path_parent, file_name = os.path.split(full_path)
        logger.info('write:path="{}", full_path="{}", full_path_parent="{}"'.format(path, full_path, full_path_parent))
        #                       http://site url/_api/web/GetFolderByServerRelativeUrl('/Folder Name')/Files/add(url='a.txt',overwrite=true)
        bio = BytesIO()
        shutil.copyfileobj(stream, bio)
        bio.seek(0)
        data = bio.read()
        headers = {
            "Content-Length": "{}".format(len(data))
        }
        response = self.client.post(
            "https://{}.sharepoint.com/sites/{}/_api/Web/GetFolderByServerRelativePath(decodedurl='/sites/dssplugin/Shared%20Documents{}')/Files/add(url='{}',overwrite=true)".format(
                self.sharepoint_tenant,
                self.sharepoint_site,
                full_path_parent,
                file_name
            ),
            headers=headers,
            data=data
        )
        logger.info("write:response={}".format(response))
