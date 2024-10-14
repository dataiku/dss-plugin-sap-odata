from dataiku.connector import Connector
from dataikuapi.utils import DataikuException
from odata_client import ODataClient
from odata_common import get_clean_row_method, get_list_title, RecordsLimit, get_sap_mode
import logging


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
        logger.info("Starting SAP-OData v1.0.4-beta.3")
        self.odata_list_title = get_list_title(config)
        self.bulk_size = config.get("bulk_size", 1000)
        self.odata_filter_query = ""
        self.sap_mode = get_sap_mode(config)

        if config.get("show_advanced_parameters", False):
            self.odata_filter_query = config.get("odata_filter_query", "")

        self.clean_row = get_clean_row_method(config)
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
        limit = RecordsLimit(records_limit=records_limit)
        skip = None
        bulk_size = self.get_bulk_size(records_limit=records_limit)
        items, next_page_url = self.client.get_entity_collections(
            entity=self.odata_list_title,
            top=bulk_size,
            skip=skip,
            filter=self.odata_filter_query
        )
        while items:
            number_of_items = len(items)
            for item in items:
                yield self.clean_row(item)
                if limit.is_reached():
                    logger.info("Limit is reached")
                    return
            if self.is_client_side_pagination():
                if skip is None:
                    skip = 0
                skip = skip + number_of_items
            else:
                if not next_page_url:
                    # Server side pagination with no next_page_url
                    # -> time to quit
                    return
            items, next_page_url = self.client.get_entity_collections(
                entity=self.odata_list_title, top=bulk_size, skip=skip,
                page_url=next_page_url, filter=self.odata_filter_query, can_raise=False
            )

    def get_bulk_size(self, records_limit=None):
        if self.is_client_side_pagination():
            if self.bulk_size == 0:
                return None
            bulk_size = self.bulk_size
            if records_limit > 0:
                bulk_size = records_limit if records_limit < bulk_size else bulk_size
        else:
            bulk_size = None
        return bulk_size

    def is_client_side_pagination(self):
        return self.sap_mode == "cds"

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
        raise DataikuException("Unimplemented")

    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise DataikuException("Unimplemented")

    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise DataikuException("unimplemented")

    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise DataikuException("unimplemented")
