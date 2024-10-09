import requests
import logging
from odata_constants import ODataConstants
from dss_constants import DSSConstants
from odata_common import get_odata_instance, get_list_title, get_login
from dataikuapi.utils import DataikuException
from time import sleep

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='sap-odata plugin %(levelname)s - %(message)s')


class ODataClient():

    MAX_RETRIES = 3

    def __init__(self, config):
        self.auth_type = config.get(DSSConstants.AUTH_TYPE)
        login = get_login(config)
        if not login:
            raise Exception("Select a valid preset")
        self.odata_instance = get_odata_instance(config)
        self.ignore_ssl_check = login.get("ignore_ssl_check", False)
        self.odata_list_title = get_list_title(config)
        odata_version = login.get(ODataConstants.VERSION)
        self.set_odata_protocol_version(odata_version)

        if "sap-odata_oauth" in config and ODataConstants.OAUTH in config["sap-odata_oauth"]:
            self.odata_access_token = config.get("sap-odata_oauth")[ODataConstants.OAUTH]
        else:
            self.odata_access_token = None
        self.session = self.get_session(config, odata_version)
        self.retries = 0

    def set_odata_protocol_version(self, odata_version):
        if odata_version == ODataConstants.ODATA_V4:
            self.force_json = False
            self.json_in_query_string = False
            self.data_container = ODataConstants.DATA_CONTAINER_V4
        if odata_version == ODataConstants.ODATA_VSAP:
            self.force_json = True
            self.json_in_query_string = True
            self.data_container = ODataConstants.DATA_CONTAINER_V2
        if odata_version == ODataConstants.ODATA_V3:
            self.force_json = True
            self.json_in_query_string = True
            self.data_container = ODataConstants.DATA_CONTAINER_V3
        if odata_version == ODataConstants.ODATA_V2:
            self.force_json = True
            self.json_in_query_string = False
            self.data_container = ODataConstants.DATA_CONTAINER_V2

    def get_session(self, config, odata_version):
        session = requests.Session()
        if self.ignore_ssl_check is True:
            session.verify = False
        login_config = config.get(ODataConstants.LOGIN, {}) if self.auth_type == "login" else config.get("sap-odata_user-account", {})
        if odata_version == ODataConstants.ODATA_VSAP:
            if self.auth_type == "user-account":
                username_password = login_config.get("username_password", {})
                session.auth = (
                    username_password.get("user", ""),
                    username_password.get("password", "")
                )
            else:
                session.auth = (
                    login_config.get(ODataConstants.USERNAME, ""),
                    login_config.get(ODataConstants.PASSWORD, "")
                )
            self.sap_client = login_config.get(ODataConstants.SAP_CLIENT, "")
            session.head(
                self.odata_instance,
                params={
                    ODataConstants.SAP_CLIENT_HEADER: self.sap_client
                }
            )
        elif ODataConstants.LOGIN in config and \
            ODataConstants.USERNAME in config[ODataConstants.LOGIN] and \
                ODataConstants.PASSWORD in config[ODataConstants.LOGIN]:
            session.auth = (
                login_config.get(ODataConstants.USERNAME, ""),
                login_config.get(ODataConstants.PASSWORD, "")
            )
        return session

    def get_entity_collections(self, entity="", top=None, skip=None, page_url=None, filter=None, can_raise=True):
        if entity is None:
            entity = ""
        if self.odata_list_title is None or self.odata_list_title == "":
            top = None  # SAP will complain if $top is present in a request to list entities
        query_options = self.get_base_query_options(top=top, skip=skip, filter=filter)
        url = page_url if page_url else self.odata_instance + '/' + entity.strip("/") + self.get_query_string(query_options)
        data = None
        while self._should_retry(data):
            logger.info("requests get url {}".format(url))
            response = self.get(url)
            if self.assert_response_ok(response, can_raise=can_raise):
                data = response.json()
            else:
                return {}, None
            data = response.json()
        next_page_url = data.get(ODataConstants.NEXT_LINK_SAP, data.get(ODataConstants.NEXT_LINK, None))
        item = data.get(ODataConstants.DATA_CONTAINER_V4, data.get(ODataConstants.DATA_CONTAINER_V2, {}))
        return self.format(item), next_page_url

    def _should_retry(self, data):
        if data is None:
            self.retries = 0
            return True
        self.retries += 1
        if "error" in data:
            if "message" in data["error"] and "value" in data["error"]["message"]:
                # SAP error causing troubles: {'error': {'code': '/IWBEP/CM_MGW_RT/004', 'message': {value': 'Metadata cache on
                if self.retries < self.MAX_RETRIES:
                    logging.warning("Remote service error : {}. Attempt {}, trying again".format(data["error"]["message"]["value"], self.retries))
                    sleep(2)
                    return True
                else:
                    logging.error("Remote service error : {}. Attempt {}, stop trying.".format(data["error"]["message"]["value"], self.retries))
                    raise DataikuException("Remote service error : {}".format(data["error"]["message"]["value"]))
            else:
                logging.error("Remote service error")
                raise DataikuException("Remote service error")
        return False

    def get(self, url, headers={}):
        headers = self.get_headers()
        args = {
            "headers": headers
        }
        if self.ignore_ssl_check is True:
            args["verify"] = False
        logger.info("Accessing endpoint {}".format(url))
        try:
            ret = self.session.get(url, **args)
            return ret
        except Exception as err:
            logging.error('error:{}'.format(err))

    def get_headers(self):
        headers = {}
        if self.force_json:
            headers["accept"] = DSSConstants.CONTENT_TYPE
        headers["Authorization"] = self.get_authorization_bearer()
        return headers

    def get_base_query_options(self, top=None, skip=None, records_limit=None, filter=None):
        if self.force_json and self.json_in_query_string:
            query_options = [DSSConstants.JSON_FORMAT]
        else:
            query_options = []
        if records_limit is not None and int(records_limit) > 0:
            query_options.append(
                ODataConstants.RECORD_LIMIT.format(records_limit)
            )
        if skip:
            query_options.append(ODataConstants.SKIP.format(skip))
        if top:
            query_options.append(ODataConstants.TOP.format(top))
        if filter:
            query_options.append(ODataConstants.FILTER.format(filter))
        return query_options

    def format(self, item):
        if ODataConstants.ENTITYSETS in item:
            rows = item[ODataConstants.ENTITYSETS]
            ret = []
            for row in rows:
                ret.append({ODataConstants.ENTITYSETS: row})
            return ret
        if ODataConstants.DATA_RESULTS in item:
            ret = item[ODataConstants.DATA_RESULTS]
        else:
            ret = item
        if isinstance(ret, list):
            return ret
        else:
            return [ret]

    def get_authorization_bearer(self):
        if self.odata_access_token is not None:
            return DSSConstants.AUTHORISATION_BEARER.format(self.odata_access_token)
        else:
            return None

    def get_query_string(self, query_options):
        if isinstance(query_options, list) and len(query_options) > 0:
            return "?" + "&".join(query_options)
        else:
            return ""

    def assert_response_ok(self, response, can_raise=True):
        status_code = response.status_code
        return_code = True
        if status_code == 404:
            return_code = False
            logger.error("Error 404, response={}".format(response.content))
            if can_raise:
                raise DataikuException("This entity does not exist")
        if status_code == 403:
            raise DataikuException("{}".format(response))
        if status_code == 401:
            raise DataikuException("Forbidden access")
        if status_code == 400:
            return_code = False
            logger.error("Error 400, response={}".format(response.content))
            if can_raise:
                raise DataikuException("Error 400: {}".format(response))
        return return_code
