from six.moves import xrange
from dataiku.connector import Connector
import sharepy, logging

from sharepoint_client import SharePointClient, SharePointSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='sharepoint plugin %(levelname)s - %(message)s')

"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant.
"""
class SharePointListsConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.sharepoint_list_title = self.config.get("sharepoint_list_title")
        self.auth_type = config.get('auth_type')
        logger.info('init:sharepoint_list_title={}, auth_type={}'.format(self.sharepoint_list_title, self.auth_type))
        self.columns={}
        self.client = SharePointClient(config)

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        If you do provide a schema here, all columns defined in the schema
        will always be present in the output (with None value),
        even if you don't provide a value in generate_rows

        The schema must be a dict, with a single key: "columns", containing an array of
        {'name':name, 'type' : type}.

        Example:
            return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

        Supported types are: string, int, bigint, float, double, date, boolean
        """

        logger.info('get_read_schema ')
        response = self.client.get_list_fields(self.sharepoint_list_title)
        if "d" not in response or "results" not in response["d"] or len(response["d"]["results"]) < 1:
            return None
        columns = []
        self.columns={}
        for column in response["d"]["results"]:
            if column['Hidden'] == False and column['ReadOnlyField']==False:
                sharepoint_type = self.get_dss_types(column["TypeAsString"])
                if sharepoint_type is not None:
                    columns.append({
                        "name": column["Title"],
                        "type": self.get_dss_types(column["TypeAsString"])
                    })
                    self.columns[column["Title"]] = sharepoint_type
        return {"columns":columns}

    def get_dss_types(self, sharepoint_type):
        SHAREPOINT_TYPES = {
            "Text" : "string",
            "Number" : "string",
            "DateTime" : "date",
            "Boolean" : "string",
            "URL" : "object",
            "Location" : "object",
            "Computed" : None,
            "Attachments" : None
        }
        if sharepoint_type in SHAREPOINT_TYPES:
            return SHAREPOINT_TYPES[sharepoint_type]
        else:
            return "string"

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        if self.columns=={}:
            self.get_read_schema()

        logger.info('generate_row:dataset_schema={}, dataset_partitioning={}, partition_id={}'.format(dataset_schema, dataset_partitioning, partition_id))

        response = self.client.get_list_all_items(self.sharepoint_list_title)
        if "d" not in response or "results" not in response["d"]:
            if "error" in response and "message" in response["error"] and "value" in response["error"]["message"]:
                raise Exception ("Error: {}".format(response["error"]["message"]["value"]))
            else:
                raise Exception("Error when interacting with SharePoint")

        for item in response["d"]["results"]:
            yield self.matched_item(item)

    def matched_item(self, item):
        ret = {}
        for key, value in item.items():
            if key in self.columns:
                ret[key] = value
        return ret

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        return SharePointListWriter(self.config, self, dataset_schema, dataset_partitioning, partition_id)


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        logger.info('get_partitioning')
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        logger.info('list_partitions:partitioning={}'.format(partitioning))
        return []


    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        logger.info('partition_exists:partitioning={}, partition_id={}'.format(partitioning, partition_id))
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        logger.info('get_records_count:partitioning={}, partition_id={}'.format(partitioning, partition_id))
        raise Exception("unimplemented")


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
        self.columns = dataset_schema["columns"]

    def write_row(self, row):
        """
        Row is a tuple with N + 1 elements matching the schema passed to get_writer.
        The last element is a dict of columns not found in the schema
        """
        logger.info('write_row:row={}'.format(row))
        self.buffer.append(row)

    def flush(self):
        self.parent.client.delete_list(self.parent.sharepoint_list_title)
        self.parent.client.create_list(self.parent.sharepoint_list_title)

        self.parent.get_read_schema()
        for column in self.columns:
            if column['name'] not in self.parent.columns:
                self.parent.client.create_custom_field(self.parent.sharepoint_list_title, column["name"])

        for row in self.buffer:
            item = self.build_row_dicttionary(row)
            self.parent.client.add_list_item(self.parent.sharepoint_list_title, item)

    def build_row_dicttionary(self, row):
        ret = {}
        for column, structure in zip(row, self.columns):
            ret[structure["name"].replace(" ", "_x0020_")] = column
        return ret

    def close(self):
        self.flush()

