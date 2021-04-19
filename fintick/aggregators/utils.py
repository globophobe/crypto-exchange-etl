import re

AGGREGATED_REGEX = re.compile(r"^(\w+\.\w+)_aggregated$")
HOT_AGGREGATED_REGEX = re.compile(r"^(\w+\.\w+)_hot_aggregated$")


def assert_aggregated_table(table_id, hot=False):
    match = AGGREGATED_REGEX.match(table_id)
    assert match, f'"source_table" {table_id} is not aggregated'
    if hot:
        table = match.group(1)
        return f"{table}_hot_aggregated"
    return table_id


def strip_hot_from_aggregated(table_id):
    match = HOT_AGGREGATED_REGEX.match(table_id)
    assert match, f'"source_table" {table_id} is not hot aggregated'
    table = match.group(1)
    return f"{table}_aggregated"


def get_firestore_collection(table_id):
    _, table_name = table_id.split(".")
    return table_name.replace("_", "-")


def get_decimal_value_for_table_id(value):
    string = str(value)
    if "." in string:
        head, tail = str(value).split(".")
        if any([int(item) for item in tail]):
            return f"{head}d{tail}"
        return head
    return string
