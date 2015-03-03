"""
JSON serializer which can encode/decode datetime instances

>>> partytime = datetime.datetime(year=1999, month=1, day=1)
>>> partytime == loads(dumps(partytime))
True
"""

from __future__ import absolute_import, print_function
from functools import partial

import json
import datetime


__author__ = 'bergundy'
__format__ = '%Y-%m-%d %H:%M:%S'


class DTEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': obj.strftime(__format__)}
        return json.JSONEncoder.default(self, obj)


def decode_datetime(obj):
    if '__datetime__' in obj:
        return datetime.datetime.strptime(obj['__datetime__'], __format__)
    return obj


DTDecoder = partial(json.JSONDecoder, object_hook=decode_datetime)

dumps = partial(json.dumps, cls=DTEncoder)
dump = partial(json.dump, cls=DTEncoder)
loads = partial(json.loads, object_hook=decode_datetime)
load = partial(json.load, object_hook=decode_datetime)

