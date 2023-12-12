import json
import re
import datetime
from dss_constants import DSSConstants
from odata_constants import ODataConstants


odata_data_pattern = re.compile(r'(?:/Date\()(-?\d+)(?:\)/)')


def clean_json_and_date(item):
    for key in DSSConstants.KEYS_TO_REMOVE:
        if key in item:
            del item[key]
    for key in item:
        value = item.get(key)
        if isinstance(value, dict):
            item[key] = json.dumps(value)
        elif isinstance(value, str):
            item[key] = convert_odata_date_to_dss(value)
    return item


def clean_json(item):
    for key in DSSConstants.KEYS_TO_REMOVE:
        if key in item:
            del item[key]
    for key in item:
        value = item.get(key)
        if isinstance(value, dict):
            item[key] = json.dumps(value)
    return item


def convert_odata_date_to_dss(input_string):
    if type(input_string) is not str:
        return input_string
    match = odata_data_pattern.match(input_string)
    if match:
        epoch_timestamp = int(match.group(1))
        epoch_timestamp /= 1000
        return datetime.datetime.utcfromtimestamp(epoch_timestamp).strftime(DSSConstants.DATE_FORMAT)
    return input_string


def get_clean_row_method(config):
    clean_row = clean_json_and_date  # New default behaviour
    should_convert_date = config.get("should_convert_date")
    if config.get("show_advanced_parameters", False):
        if should_convert_date is False:
            clean_row = clean_json
    if should_convert_date is None:
        # old version of UI -> don't break flows
        clean_row = clean_json  # Default to old behaviour
    return clean_row


def get_login(config):
    login = {}
    auth_type = config.get(DSSConstants.AUTH_TYPE)
    login = config.get('sap-odata_{}'.format(auth_type), {})
    return login


def get_odata_instance(config):
    odata_instance = ""
    odata_service_node = config.get("odata_service_node_select", "").strip("/")
    login = get_login(config)

    if odata_service_node == ODataConstants.UI_MANUAL_SELECT:
        odata_service_node = config.get(ODataConstants.SERVICE_NODE, "").strip("/")
    if odata_service_node != "":
        odata_instance = "/".join([login.get(ODataConstants.INSTANCE, "").strip("/"), odata_service_node])
    else:
        odata_instance = login.get(ODataConstants.INSTANCE, "").strip("/")
    return odata_instance


def get_list_title(config):
    odata_list_title = None
    odata_list_title = config.get("odata_list_selector")
    if odata_list_title == ODataConstants.UI_MANUAL_SELECT:
        odata_list_title = config.get(ODataConstants.LIST_TITLE)
    return odata_list_title


class DSSSelectorChoices(object):
    def __init__(self):
        self.choices = []

    def append(self, label, value):
        self.choices.append(
            {
                "label": label,
                "value": value
            }
        )

    def append_manual_select(self):
        self.choices.append(
            {
                "label": "✍️ Enter manually",
                "value": ODataConstants.UI_MANUAL_SELECT
            }
        )

    def _build_select_choices(self, choices=None):
        if not choices:
            return {"choices": []}
        if isinstance(choices, str):
            return {"choices": [{"label": "{}".format(choices)}]}
        if isinstance(choices, list):
            return {"choices": choices}
        if isinstance(choices, dict):
            returned_choices = []
            for choice_key in choices:
                returned_choices.append({
                    "label": choice_key,
                    "value": choices.get(choice_key)
                })

    def text_message(self, text_message):
        return self._build_select_choices(text_message)

    def to_dss(self):
        return self._build_select_choices(self.choices)
