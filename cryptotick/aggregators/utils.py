from ..utils import publish


def publish_post_aggregation(table_name, date, post_aggregation):
    for options in post_aggregation:
        function = options.pop("function", None)
        data = {"table_name": table_name, "date": date}
        data.update(options)
        publish(function, data)
