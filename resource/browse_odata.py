from odata_client import ODataClient
from odata_common import get_login, DSSSelectorChoices


def get_odata_list_selector(payload, config, plugin_config, inputs):
    choices = DSSSelectorChoices()
    client = ODataClient(config)
    items, _ = client.get_entity_collections(
        entity=""
    )
    choices.append_manual_select()
    for item in items:
        entity_set = item.get("EntitySets")
        choices.append("{}".format(entity_set), "{}".format(entity_set))
    return choices


def get_odata_service_node_select(payload, config, plugin_config, inputs):
    choices = DSSSelectorChoices()
    service_names = get_service_names(config)
    choices.append_manual_select()
    for service_name in service_names:
        choices.append(service_names.get(service_name), service_name)
    return choices


PARAMETER_COMPUTE_METHODS = {
    "odata_list_selector": get_odata_list_selector,
    "odata_service_node_select": get_odata_service_node_select
}


def get_service_names(config):
    service_names = {}
    login = get_login(config)
    if login:
        service_names = login.get("service_names", {})
    return service_names


def do(payload, config, plugin_config, inputs):
    """Compute the param for the given payload"""
    parameter_name = payload.get("parameterName")
    compute_method = PARAMETER_COMPUTE_METHODS.get(parameter_name)
    return compute_method(payload, config, plugin_config, inputs).to_dss()
