__author__ = 'dvirsky'

import datetime
import bson

from .errors import ColumnValueError, MeduzaError
from . import queries

class Column(object):

    NoDefault = "%%NDEF%%"

    def __init__(self, name='', default = NoDefault, required=False, choices = None):

        self.name = name

        self.primary = False
        self.required = bool(required)
        self.hasDefault = False
        self._default = None
        self.modelName = name

        if default != Column.NoDefault:

            self._default = default
            self.hasDefault = True

        self._choices = set(choices) if choices is not None else None

    def default(self):

        if callable(self._default):
            return self._default()

        return self._default



    def toFilter(self, condition, values):


        if isinstance(values, (tuple, list, set)):
            return queries.Filter(self.name, condition, *values)
        else:
            return queries.Filter(self.name, condition, values)

    def __eq__(self, other):

        return self.toFilter(queries.Condition.EQ, other)

    def __gt__(self, other):

        return self.toFilter(queries.Condition.GT, other)

    def IN(self, *other):
        return self.toFilter(queries.Condition.IN, other)



    def validateChoices(self, data):

        if self._choices is not None and data not in self._choices:
            raise ColumnValueError("%s not in choices for %s" %(data, self.name))



NIL = '{NIL}'


class Key(Column):

    def __init__(self, name):

        Column.__init__(self, name)
        self.primary = True


    def decode(self, data):


        if data == NIL or data is None:
            return None

        elif isinstance(data, unicode):
            return data.encode('utf-8')

        return '%s' % data

    def encode(self, data):

        if data == NIL or data is None:
            return None

        if isinstance(data, unicode):
            return data.encode('utf-8')
        elif isinstance(data, bson.Binary):
            return str(data)

        return '%s' % data




class Text(Column):
    """
    Representing a text column
    """


    def __init__(self, name='', maxLen=-1, **kwargs):

        Column.__init__(self, name=name, **kwargs)
        self.maxLen = maxLen


    def decode(self, data):

        if data == NIL or data is None:
            return None

        #check that the lendth is not too big
        if 0 < self.maxLen < len(data):
            raise ColumnValueError("Value for %s too large, allowed %d, have %d" % (self.name, self.maxLen, len(data)))

        if isinstance(data, unicode):
            return data.encode('utf-8')

        return '%s' % data

    def encode(self, data):

        if data is None or data == NIL:
            return None

        # encode unicode to utr
        if isinstance(data, unicode):
            data = data.encode('utf-8')

        #encode everything else to str
        elif not isinstance(data, str):
            data = '%s' % data


        return data


class Int(Column):
    """ Representing an integer column """
    def decode(self, data):
        return int(data) if data is not None else None

    def encode(self, data):
        return int(data) if data is not None else None

import math

class Uint(Column):
    """
    Representing an unsigned integer column
    """
    def decode(self, data):
        long(-data if data < 0 else data) if data is not None else None

    def encode(self, data):
        long(-data if data < 0 else data) if data is not None else None

class Float(Column):
    """ Representing an integer column """
    def decode(self, data):
        return float(data) if data is not None else None

    def encode(self, data):
        return float(data) if data is not None else None

class Binary(Column):
    """ Representing an integer column """
    def decode(self, data):
        return bytearray(data) if data is not None else None

    def encode(self, data):
        return bytearray(data) if data is not None else None

class Timestamp(Column):

    @classmethod
    def now(cls):

        return datetime.datetime.utcnow()


    def decode(self, data):
        if data == None:
            return None

        if isinstance(data, datetime.datetime):
            return data
        #convert time.time() based timestamps
        elif isinstance(data, (long, int, float)):
            return datetime.datetime.utcfromtimestamp(data)

        raise ColumnValueError("Invalid value for datetime: %s"%data)

    def encode(self, data):
        if data is None:
            d = self.default()
            if d is not None:
                return self.encode(d)

        return self.decode(data)



class Bool(Column):

    def decode(self, data):

        if data is None:
            return None

        if isinstance(data, bool):
            return data

        elif isinstance(data, (int, long)):
            return bool(data)

        if isinstance(data, basestring):
            return data == "1" or data.lower() == "true"

    def encode(self, data):
        return self.decode(data)


class Set(Column):
    """
    Representing a set column
    """

    IDENT = '__MDZS__'

    def __init__(self, name, type=None, default = None):


        Column.__init__(self, name, default=default)
        self._type = type


    def decode(self, data):

        if data == NIL or data is None:
            return None

        if not isinstance(data, (list, tuple)):
            raise MeduzaError("Invalid type for decoded set: %s", type(data))

        return set(filter(lambda x: x != self.IDENT, (self._type.decode(e) for e in data)))


    def encode(self, data):

        if data is None or data == NIL:
            return None

        if not isinstance(data, set):
            raise ValueError("Invalid data for set: %s", type(data))

        return [self.IDENT] + list(data)



class List(Column):
    """
    Representing a list column
    """

    IDENT = '__MDZL__'

    def __init__(self, name, type=None, default = None):


        Column.__init__(self, name, default=default)
        self._type = type


    def decode(self, data):

        if data == NIL or data is None:
            return None

        if not isinstance(data, (list, tuple)):
            raise MeduzaError("Invalid type for decoded set: %s", type(data))


        return filter(lambda x: x != self.IDENT, (self._type.decode(e) for e in data))


    def encode(self, data):

        if data is None or data == NIL:
            return None

        if not isinstance(data, (list, tuple)):
            raise ValueError("Invalid data for set: %s", type(data))

        ret = [self.IDENT] + list(data)

        return ret


class Map(Column):
    """
    Representing a list column
    """

    def __init__(self, name, type=None, default = None):


        Column.__init__(self, name, default=default)
        self._type = type


    def decode(self, data):

        if data == NIL or data is None:
            return None

        if not isinstance(data, dict):
            raise MeduzaError("Invalid type for decoded set: %s", type(data))

        return {k: self._type.decode(v) for k,v in data.iteritems()}



    def encode(self, data):

        if data is None or data == NIL:
            return None

        if not isinstance(data, dict):
            raise ValueError("Invalid data for set: %s", type(data))

        return {k: self._type.encode(v) for k,v in data.iteritems()}

