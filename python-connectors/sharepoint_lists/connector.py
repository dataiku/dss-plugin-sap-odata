from six.moves import xrange
from dataiku.connector import Connector
import sharepy, logging

from sharepoint_client import SharePointClient, SharePointSession

from sharepoint_client import *
from dss_constants import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='sharepoint plugin %(levelname)s - %(message)s')

class SharePointListsConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.sharepoint_list_title = self.config.get("sharepoint_list_title")
        self.auth_type = config.get('auth_type')
        logger.info('init:sharepoint_list_title={}, auth_type={}'.format(self.sharepoint_list_title, self.auth_type))
        self.columns={}
        self.client = SharePointClient(config)

    def get_read_schema(self):
        logger.info('get_read_schema ')
        response = self.client.get_list_fields(self.sharepoint_list_title)
        if self.is_response_empty(response) or len(response[SHAREPOINT_RESULTS_CONTAINER_V2][SHAREPOINT_RESULTS]) < 1:
            return None
        columns = []
        self.columns={}
        for column in self.result_loop(response):
            if column[SHAREPOINT_HIDDEN_COLUMN] == False and column[SHAREPOINT_READ_ONLY_FIELD]==False:
                sharepoint_type = self.get_dss_types(column[SHAREPOINT_TYPE_AS_STRING])
                if sharepoint_type is not None:
                    columns.append({
                        "name": column[SHAREPOINT_TITLE_COLUMN],
                        "type": self.get_dss_types(column[SHAREPOINT_TYPE_AS_STRING])
                    })
                    self.columns[column[SHAREPOINT_TITLE_COLUMN]] = sharepoint_type
        return {"columns":columns}

    def get_dss_types(self, sharepoint_type):
        if sharepoint_type in SHAREPOINT_TYPES:
            return SHAREPOINT_TYPES[sharepoint_type]
        else:
            return "string"

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        if self.columns=={}:
            self.get_read_schema()

        logger.info('generate_row:dataset_schema={}, dataset_partitioning={}, partition_id={}'.format(
            dataset_schema, dataset_partitioning, partition_id
        ))

        response = self.client.get_list_all_items(self.sharepoint_list_title)
        if self.is_response_empty(response):
            if self.is_error(response):
                raise Exception ("Error: {}".format(response[SHAREPOINT_ERROR_CONTAINER][SHAREPOINT_MESSAGE][SHAREPOINT_VALUE]))
            else:
                raise Exception("Error when interacting with SharePoint")

        for item in self.result_loop(response):
            yield self.matched_item(item)

    def matched_item(self, item):
        ret = {}
        for key, value in item.items():
            if key in self.columns:
                ret[key] = value
        return ret

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        return SharePointListWriter(self.config, self, dataset_schema, dataset_partitioning, partition_id)


    def get_partitioning(self):
        logger.info('get_partitioning')
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        logger.info('list_partitions:partitioning={}'.format(partitioning))
        return []


    def partition_exists(self, partitioning, partition_id):
        logger.info('partition_exists:partitioning={}, partition_id={}'.format(partitioning, partition_id))
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        logger.info('get_records_count:partitioning={}, partition_id={}'.format(partitioning, partition_id))
        raise Exception("unimplemented")

    def result_loop(self, response):
        return response[SHAREPOINT_RESULTS_CONTAINER_V2][SHAREPOINT_RESULTS]

    def is_response_empty(self, response):
        return SHAREPOINT_RESULTS_CONTAINER_V2 not in response or SHAREPOINT_RESULTS not in response[SHAREPOINT_RESULTS_CONTAINER_V2]

    def is_error(self, response):
        return SHAREPOINT_ERROR_CONTAINER in response and SHAREPOINT_MESSAGE in response[SHAREPOINT_ERROR_CONTAINER] and SHAREPOINT_VALUE in response[SHAREPOINT_ERROR_CONTAINER][SHAREPOINT_MESSAGE]

class SharePointListWriter(object):

    APPLICATION_JSON = "application/json;odata=verbose"

    def __init__(self, config, parent, dataset_schema, dataset_partitioning, partition_id):
        self.parent = parent
        self.config = config
        self.dataset_schema = dataset_schema
        self.dataset_partitioning = dataset_partitioning
        self.partition_id = partition_id
        self.buffer = []
        logger.info('init SharepointListWriter')
        self.columns = dataset_schema[SHAREPOINT_COLUMNS]

    def write_row(self, row):
        logger.info('write_row:row={}'.format(row))
        self.buffer.append(row)

    def flush(self):
        self.parent.client.delete_list(self.parent.sharepoint_list_title)
        self.parent.client.create_list(self.parent.sharepoint_list_title)

        self.parent.get_read_schema()
        for column in self.columns:
            if column[SHAREPOINT_NAME_COLUMN] not in self.parent.columns:
                self.parent.client.create_custom_field(self.parent.sharepoint_list_title, column[SHAREPOINT_NAME_COLUMN])

        for row in self.buffer:
            item = self.build_row_dicttionary(row)
            self.parent.client.add_list_item(self.parent.sharepoint_list_title, item)

    def build_row_dicttionary(self, row):
        ret = {}
        for column, structure in zip(row, self.columns):
            ret[structure[SHAREPOINT_NAME_COLUMN].replace(" ", "_x0020_")] = column
        return ret

    def close(self):
        self.flush()

