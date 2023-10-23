import json
import re
import datetime
from dss_constants import DSSConstants

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
