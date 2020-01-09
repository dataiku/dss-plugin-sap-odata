import os, requests, shutil, sharepy

try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

class SharePointClient():

    APPLICATION_JSON = "application/json;odata=verbose"

    def __init__(self, config):
        if config.get('auth_type') == "oauth":
            self.sharepoint_tenant = config.get('sharepoint_oauth')['sharepoint_tenant']
            self.sharepoint_site = config.get('sharepoint_oauth')['sharepoint_site']
            self.sharepoint_access_token = config.get('sharepoint_oauth')['sharepoint_oauth']
            self.session = SharePointSession(
                None,
                None,
                self.sharepoint_tenant,
                self.sharepoint_site,
                sharepoint_access_token = self.sharepoint_access_token
            )
        else:
            username = config.get('sharepoint_sharepy')['sharepoint_username']
            password = config.get('sharepoint_sharepy')['sharepoint_password']
            self.sharepoint_tenant = config.get('sharepoint_sharepy')['sharepoint_tenant']
            self.sharepoint_site = config.get('sharepoint_sharepy')['sharepoint_site']
            self.sharepoint_url = self.sharepoint_tenant + ".sharepoint.com"
            self.session = sharepy.connect(self.sharepoint_url, username=username, password=password)
        self.sharepoint_list_title = config.get("sharepoint_list_title")

    def get_folders(self, path):
        return self.session.get(self.get_sharepoint_item_url(path) + "/Folders" ).json()

    def get_files(self, path):
        return self.session.get(self.get_sharepoint_item_url(path) + "/Files" ).json()

    def get_sharepoint_item_url(self, path):
        URL_STRUCTURE = "https://{0}.sharepoint.com/sites/{1}/_api/Web/GetFolderByServerRelativePath(decodedurl='/sites/{1}/Shared%20Documents{2}')"
        if path == '/':
            path = ""
        return URL_STRUCTURE.format(self.sharepoint_tenant, self.sharepoint_site, path)

    def get_file_content(self, full_path):
        response = self.session.get(
            self.get_file_content_url(full_path)
        )
        return response

    def write_file_content(self, full_path, data):
        full_path_parent, file_name = os.path.split(full_path)
        headers = {
            "Content-Length": "{}".format(len(data))
        }
        response = self.session.post(
            self.get_file_add_url(
                full_path_parent,
                file_name
            ),
            headers=headers,
            data=data
        )
        return response

    def create_folder(self, full_path):
        response = self.session.post(
            self.get_add_folder_url(full_path)
        )
        return response

    def move_file(self, full_from_path, full_to_path):
        response = self.session.post(
            self.get_move_url(
                full_from_path,
                full_to_path
            )
        )
        return response.json()

    def delete_file(self, full_path):
        headers = {
            "X-HTTP-Method":"DELETE"
        }
        response = self.session.post(
            self.get_file_relative_url(full_path),
            headers = headers
        )

    def delete_folder(self, full_path):
        headers = {
            "X-HTTP-Method":"DELETE"
        }
        response = self.session.post(
            self.get_folder_relative_url(full_path),
            headers = headers
        )

    def get_list_fields(self, list_title):
        response = self.session.get(
            self.get_list_fields_url(list_title)
        ).json()
        return response

    def get_list_all_items(self, list_title):
        items = self.get_list_items(list_title)
        buffer = items
        while "d" in items and "__next" in items["d"]:
            items = self.session.get(items["d"]["__next"]).json()
            buffer["d"]["results"].extend(items["d"]["results"])
        return buffer

    def get_list_items(self, list_title):
        response = self.session.get(
            self.get_list_items_url(list_title)
        ).json()
        return response

    def create_list(self, list_name):
        headers={
            "content-type": self.APPLICATION_JSON,
            'Accept': 'application/json; odata=nometadata'
        }
        data = {
            '__metadata': {
                'type': 'SP.List'
            },
            'AllowContentTypes': True,
            'BaseTemplate': 100,
            'ContentTypesEnabled': True,
            'Title': list_name
        }
        response = self.session.post(
            self.get_lists_url(),
            headers=headers,
            json=data
        )
        return response

    def delete_list(self, list_name):
        headers={
            "X-HTTP-Method": "DELETE",
            "IF-MATCH" : "*"
        }
        response = self.session.post(
            self.get_lists_by_title_url(list_name),
            headers = headers
        )
        return response

    def create_custom_field(self, list_title, field_title):
            body = {
                'parameters' : {
                    '__metadata': { 'type': 'SP.XmlSchemaFieldCreationInformation' },
                    'SchemaXml':"<Field DisplayName='{0}' Format='Dropdown' MaxLength='255' Name='{0}' Title='{0}' Type='Text'></Field>".format(field_title)
                }
            }
            headers = {
                "content-type": self.APPLICATION_JSON
            }
            response = self.session.post(
                self.get_lists_add_field_url(list_title),
                headers = headers,
                json=body
            )
            return response

    def add_list_item(self, list_title, item):
        item["__metadata"] = {
            "type" : "SP.Data.{}ListItem".format(list_title.capitalize())
        }
        headers = {
            "Content-Type": self.APPLICATION_JSON
        }
        response = self.session.post(
            self.get_list_items_url(list_title),
            json=item,
            headers=headers
        )
        return response

    def get_base_url(self):
        return "https://{}.sharepoint.com/sites/{}/_api/Web".format(
            self.sharepoint_tenant, self.sharepoint_site
        )

    def get_lists_url(self):
        return self.get_base_url() + "/lists"

    def get_lists_by_title_url(self, list_title):
        return self.get_lists_url() + "/GetByTitle('{}')".format(list_title)

    def get_list_items_url(self, list_title):
        return self.get_lists_by_title_url(list_title) + "/Items"

    def get_list_fields_url(self, list_title):
        return self.get_lists_by_title_url(list_title) + "/fields"

    def get_lists_add_field_url(self, list_title):
        return self.get_base_url() + "/GetList(@a1)/Fields/CreateFieldAsXml?@a1='/sites/{}/Lists/{}'".format(
            self.sharepoint_site,
            list_title
        )

    def get_folder_url(self, full_path):
        return self.get_base_url() + "/GetFolderByServerRelativePath(decodedurl={})".format(
            self.get_site_path(full_path)
        )

    def get_file_url(self, full_path):
        return self.get_base_url() + "/GetFileByServerRelativePath(decodedurl={})".format(
            self.get_site_path(full_path)
        )

    def get_file_content_url(self,full_path):
        return self.get_file_url(full_path) + "/$value"

    def get_file_relative_url(self, full_path):
        return self.get_base_url() + "/GetFileByServerRelativeUrl({})".format(
            self.get_site_path(full_path)
        )

    def get_move_url(self, from_path, to_path):
        return self.get_file_relative_url(from_path) + "/moveto(newurl={},flags=1)".format(
            self.get_site_path(to_path)
        )

    def get_folder_relative_url(self, full_path):
        return self.get_base_url() + "/GetFolderByServerRelativeUrl({})".format(
            self.get_site_path(full_path)
        )

    def get_site_path(self, full_path):
        return "'/sites/{}/Shared%20Documents{}'".format(self.sharepoint_site, full_path)

    def get_add_folder_url(self, full_path):
        return self.get_base_url() + "/Folders/add('Shared%20Documents/{}')".format(
            full_path
        )

    def get_file_add_url(self, full_path, file_name):
        return self.get_folder_url(full_path) + "/Files/add(url='{}',overwrite=true)".format(file_name)

class SharePointSession():

    APPLICATION_JSON = "application/json;odata=verbose"

    def __init__(self, sharepoint_user_name, sharepoint_password, sharepoint_tenant, sharepoint_site, sharepoint_access_token = None):
        self.sharepoint_tenant = sharepoint_tenant
        self.sharepoint_site = sharepoint_site
        self.sharepoint_access_token = sharepoint_access_token

    def get(self, url, headers = {}):
        headers["accept"] = self.APPLICATION_JSON
        headers["Authorization"] = self.get_authorization_bearer()
        return requests.get(url, headers = headers)

    def post(self, url, headers = {}, json=None, data=None):
        headers["accept"] = self.APPLICATION_JSON
        headers["Authorization"] = self.get_authorization_bearer()
        return requests.post(url, headers = headers, json=json, data=data)

    def json_post(self, url, headers = {}, json=None, data=None):
        headers["accept"] = self.APPLICATION_JSON
        headers["Authorization"] = self.get_authorization_bearer()
        return requests.post(url, headers = headers, json=json)

    def get_authorization_bearer(self):
        return "Bearer {}".format(self.sharepoint_access_token)