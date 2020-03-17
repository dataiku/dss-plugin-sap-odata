from dataiku.connector import Connector
import logging

from odata_client import ODataClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='sap-odata plugin %(levelname)s - %(message)s')


class SAPODataConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)
        self.odata_list_title = self.config.get("odata_list_title")
        self.client = ODataClient(config)
        # According to https://www.odata.org/documentation/odata-version-2-0/uri-conventions/
        # https://services.odata.org/OData/OData.svc/Category(1)/Products?$top=2&$orderby=name
        # <-      service root URI                -><- resource path  -><- query options   ->

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
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                      partition_id=None, records_limit=-1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        items = self.client.get_entity_collections(self.odata_list_title, records_limit=records_limit)
        for item in items:
            yield self.clean(item)

    def clean(self, item):
        keys_to_remove = ["__metadata", "odata.type"]
        for key in keys_to_remove:
            if key in item:
                del item[key]
        return item

    def get_schema_set(self, set_name):
        for one_set in self.client.schema.entity_sets:
            if one_set.name == set_name:
                return one_set

    def get_set(self, set_name):
        for one_set in self.client.entity_sets:
            if one_set.name == set_name:
                return one_set

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                   partition_id=None):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        return ODataDatasetWriter(self.config, self, dataset_schema, dataset_partitioning, partition_id)

    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise Exception("Unimplemented")

    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")

    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")


class ODataDatasetWriter(object):
    def __init__(self, config, parent, dataset_schema, dataset_partitioning, partition_id):
        self.parent = parent
        self.config = config
        self.dataset_schema = dataset_schema
        self.dataset_partitioning = dataset_partitioning
        self.partition_id = partition_id
        self.buffer = []
        logger.info('init ODataDatasetWriter')
        self.columns = dataset_schema["columns"]

    def write_row(self, row):
        """
        Row is a tuple with N + 1 elements matching the schema passed to get_writer.
        The last element is a dict of columns not found in the schema
        """
        logger.info('write_row:row={}'.format(row))
        self.buffer.append(row)

    def close(self):
        self.flush()

    def flush(self):
        self.parent.service.delete_list(self.parent.odata_list_title)
        self.parent.service.save(self.parent.odata_list_title)

        self.parent.get_read_schema()
        for column in self.columns:
            if column['name'] not in self.parent.columns:
                self.parent.service.create_custom_field(self.parent.odata_list_title, column["name"])

        for row in self.buffer:
            item = self.build_row_dictionary(row)
            self.parent.service.add_list_item(self.parent.odata_list_title, item)

    def build_row_dictionary(self, row):
        ret = {}
        for column, structure in zip(row, self.columns):
            ret[structure["name"].replace(" ", "_x0020_")] = column
        return ret
