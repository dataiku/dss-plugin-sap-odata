import requests
import logging
from odata_constants import ODataConstants
from dss_constants import DSSConstants
from dataikuapi.utils import DataikuException
from time import sleep

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='sap-odata plugin %(levelname)s - %(message)s')


class ODataClient():

    MAX_RETRIES = 3

    def __init__(self, config):
        self.auth_type = config.get(DSSConstants.AUTH_TYPE)
        login = config.get('sap-odata_{}'.format(self.auth_type))
        self.odata_service_node = config.get(ODataConstants.SERVICE_NODE).strip("/")
        if self.odata_service_node != "":
            self.odata_instance = "/".join([login[ODataConstants.INSTANCE].strip("/"), self.odata_service_node])
        else:
            self.odata_instance = login[ODataConstants.INSTANCE].strip("/")
        self.ignore_ssl_check = login["ignore_ssl_check"]
        self.odata_list_title = config.get(ODataConstants.LIST_TITLE)
        odata_version = login[ODataConstants.VERSION]
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
        login_config = config.get(ODataConstants.LOGIN)
        if odata_version == ODataConstants.ODATA_VSAP:
            self.sap_client = login_config.get(ODataConstants.SAP_CLIENT, "")
            session.auth = (
                login_config.get(ODataConstants.USERNAME, ""),
                login_config.get(ODataConstants.PASSWORD, "")
            )
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

    def get_entity_collections(self, entity, top=None, skip=None):
        if self.odata_list_title is None or self.odata_list_title == "":
            top = None  # SAP will complain if $top is present in a request to list entities
        query_options = self.get_base_query_options(top=top, skip=skip)
        url = self.odata_instance + '/' + entity.strip("/") + self.get_query_string(query_options)
        data = None
        while self._should_retry(data):
            response = self.get(url)
            self.assert_response(response)
            data = response.json()
        return self.format(data[self.data_container])

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

    def get_base_query_options(self, top=None, skip=None, records_limit=None):
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

    def assert_response(self, response):
        status_code = response.status_code
        if status_code == 404:
            raise DataikuException("This entity does not exist")
        if status_code == 403:
            raise DataikuException("{}".format(response))
        if status_code == 401:
            raise DataikuException("Forbidden access")
