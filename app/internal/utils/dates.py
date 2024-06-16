import datetime
from typing import Union

_format = "%Y-%m-%dT%H:%M:%S.%f%z"


def str_to_datetime(value: Union[None, str, datetime.datetime]):
    if value is None:
        return None
    elif isinstance(value, datetime.datetime):
        return value

    return datetime.datetime.strptime(value, _format)


def datetime_to_str(value: Union[None, str, datetime.datetime]):
    if value is None:
        return None
    elif isinstance(value, str):
        return value

    return value.strftime(_format)
