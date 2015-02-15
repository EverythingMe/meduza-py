
__author__ = 'dvirsky'

import queries
import logging
import datetime
import bson

class Column(object):

    def __init__(self, name, default = None):

        self.name = name
        self.primary = False
        self._default = default


    def default(self):

        if callable(self._default):
            return self._default()

        return self._default



    def toFilter(self, condition, values):


        return queries.Filter(self.name, condition, values)

    def __eq__(self, other):

        return self.toFilter(queries.Condition.EQ, other)

    def __gt__(self, other):

        return self.toFilter(queries.Condition.GT, other)

    def IN(self, *other):
        return self.toFilter(queries.Condition.IN, other)



NIL = '{NIL}'


class Key(Column):

    def __init__(self, name):

        Column.__init__(self, name)
        self.primary = True


    def decode(self, data):

        print data, type(data)
        if data == NIL or data is None:
            return None

        elif isinstance(data, unicode):
            return data.encode('utf-8')
        elif isinstance(data, bson.Binary):
            return str(data)

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

    def decode(self, data):

        if data == NIL or data is None:
            return None

        if isinstance(data, unicode):
            return data.encode('utf-8')

        return '%s' % data

    def encode(self, data):

        if data is None or data == NIL:
            return None

        if isinstance(data, unicode):
            return data.encode('utf-8')

        return '%s' % data


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

        raise ValueError("Invalid value for datetime: %s"%data)

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


def Model(table):


    class _Model(object):

        _table = table
        _columns = dict()
        _primary = None

        def __init__(self, **kwargs):

            self.__dict__.update(kwargs)

            # If the object doesn't have a primary value, put none
            if not self.__dict__.has_key(self.primary()):
                self.setPrimary(None)

        @classmethod
        def primary(cls):
            """
            Return the class' attribute name for the primary key (usually "id")
            :return:
            """

            # make sure we precache the columns and primary
            if cls._primary is None:
                cls.columns()

            return cls._primary

        @classmethod
        def columns(cls):
            """
            Return a map of the columns of the class by their internal name (not the table based name)
            :return:
            """
            columns = {}
            primary = None

            if not cls._columns:
                for  k,v in cls.__dict__.iteritems():
                    if  isinstance(v, Column):
                        columns[k] = v

                        if v.primary:
                            primary = k

                cls._columns = columns
                cls._primary = primary
            return cls._columns



        @classmethod
        def decode(cls, entity):
            """
            Decode an entity into the model's class using the column spec
            :param entity: an entity
            :return:
            """
            cols = cls.columns()

            obj = object.__new__(cls)
            obj.setPrimary(entity.Id)

            for k,v in entity.Properties.iteritems():

                if  k == cls.primary():
                    continue

                col = cols.get(k)
                if not col:
                    logging.warn("Could not map %s to object - not in model", k)
                    continue

                setattr(obj, k, col.decode(v))

            return obj


        def encode(self):
            """
            Encode an object from the model into an entity for wire transmission
            :return:
            """

            cols = self.__class__.columns()
            primary = self.primary()
            pcol = cols[self.primary()]

            ent = queries.Entity(pcol.encode(getattr(self, primary)))

            for k, col in cols.iteritems():

                if k == primary:
                    continue
                ent.Properties[k] = col.encode(self.__dict__.get(k))
                print k, ent.Properties[k]
            return ent




        def __repr__(self):

            return '%s<%s> %s' %(self._table, getattr(self, self._primary), self.__dict__)


        def setPrimary(self, val):
            """
            Set the value of the primary key
            :param val: the value
            :return:
            """
            pcol = self.columns()[self.primary()]

            setattr(self, self.primary(), pcol.decode(val))



    return _Model


