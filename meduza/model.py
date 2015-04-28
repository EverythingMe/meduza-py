from meduza.errors import ColumnValueError

__author__ = 'dvirsky'

import logging

from .columns import Column, Key
from .queries import Filter, Condition, Entity


ID = "id"


class ModelType(type):
    """
    Meta class for models, it injects the member name in a model of fields into the columns
    """
    def __new__(mcs, name, bases, dct):
        # Copy all of the base Models columns into our subclass columns
        columns = {k: v for base in bases if isinstance(base, ModelType) for k, v in base.__columns__.iteritems()}

        for k, v in dct.iteritems():
            if isinstance(v, Column):
                # Assign the modelName to the Column and register the column name in the columns dict
                # Example:
                # class MyModel(Model):
                #     clientFriendlyName = Int('serverName')
                #
                # Here the Int object's modelName will be populated with 'clientFriendlyName'
                # The columns are indexed by the server name ('serverName' in this example)
                v.modelName = k
                columns[v.name] = v

        dct['__columns__'] = columns

        for k, v in columns.iteritems():
            if v.primary:
                dct['__primary__'] = k
                break
        else:
            raise RuntimeError('Model {} has no primary key'.format(name))

        return type.__new__(mcs, name, bases, dct)


class Model(object):
    __metaclass__ = ModelType

    _table = None
    _schema = None
    __columns__ = None

    id = Key(ID)

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

        # If the object doesn't have a primary value, put none
        if self.__primary__ not in self.__dict__:
            self.setPrimary(None)

        primary = self.__primary__
        # Set the default v
        for k, col in self.__columns__.iteritems():
            if k == primary:
                continue
            if col.modelName not in self.__dict__:
                default = col.default()
                if default is not Column.Undefined:
                    setattr(self, col.modelName, default)

    def __getattribute__(self, item):
        ret = object.__getattribute__(self, item)

        if isinstance(ret, Column):
            raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, item))
        return ret

    @classmethod
    def all(cls):
        return Filter(cls.__primary__, Condition.ALL)

    @classmethod
    def primary(cls):
        """
        Return the class' attribute name for the primary key (usually "id")
        :return:
        """
        return cls.__primary__

    @classmethod
    def decode(cls, entity):
        """
        Decode an entity into the model's class using the column spec
        :param entity: an entity
        :return:
        """
        cols = cls.__columns__

        obj = object.__new__(cls)
        obj.setPrimary(entity.id)

        for k, v in entity.properties.iteritems():

            if k == cls.__primary__:
                continue

            col = cols.get(k)
            if not col:
                logging.warn("Could not map %s to object - not in model", k)
                continue

            setattr(obj, col.modelName, col.decode(v))

        return obj

    def encode(self):
        """
        Encode an object from the model into an entity for wire transmission
        :return:
        """

        cols = self.__columns__
        primary = self.__primary__
        pcol = cols[primary]

        ent = Entity(pcol.encode(getattr(self, primary)))

        for k, col in cols.iteritems():

            if k == primary:
                continue

            if col.modelName not in self.__dict__:
                if col.required:
                    raise ColumnValueError("Required column %s not set in %s" % (col.modelName, self._table))
                else:
                    continue

            data = self.__dict__.get(col.modelName)
            # print k, data
            # col.validateChoices(data)
            ent.properties[k] = col.encode(data)

        return ent

    @classmethod
    def tableName(cls):
        """
        Get the full quailified table name with its schema namespace
        :return:
        """

        return '%s.%s' % (cls._schema, cls._table)

    def __repr__(self):
        return '%s<%s> %s' % (self._table, getattr(self, self.__primary__), self.__dict__)

    def setPrimary(self, val):
        """
        Set the value of the primary key
        :param val: the value
        :return:
        """
        pcol = self.__columns__[self.__primary__]

        setattr(self, self.__primary__, pcol.decode(val))
