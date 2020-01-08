import os, requests, shutil

try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

class SharePointClient():

    APPLICATION_JSON = "application/json;odata=verbose"

    def __init__(self, sharepoint_user_name, sharepoint_password, sharepoint_tenant, sharepoint_site, list_title = None, sharepoint_access_token = None):
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

    def get_custom_field_url(self):
        return "{}/_api/web/GetList(@a1)/Fields/CreateFieldAsXml?@a1='/sites/{}/Lists/{}'".format(
            self.get_site_url(),
            self.sharepoint_site,
            self.list_title
        )

    def get_items_url(self):
        return "{}/Items".format(self.get_by_list_title_url(), self.list_title)
    
    def get_fields_url(self):
        return "{}/Fields".format(self.get_by_list_title_url(), self.list_title)
    
    def get_by_list_title_url(self):
        return "{}/GetByTitle('{}')".format(self.get_lists_url(), self.list_title)

    def get_lists_url(self):
        return "{}/_api/Web/lists".format(self.get_site_url())

    def get_shared_file_url(self, sharepoint_file_path):
        return "{}/Shared Documents{}".format(self.get_site_url(), sharepoint_file_path)

    def get_site_url(self, title = None, fields=False, items = False):
        return "https://{}/sites/{}".format(self.get_domain(), self.sharepoint_site)
    
    def get_domain(self):
        return "{}.sharepoint.com".format(self.sharepoint_tenant)

    def get_authorization_bearer(self):
        return "Bearer {}".format(self.sharepoint_access_token)