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
    @staticmethod
    def __new__(cls, name, bases, dict):
        for k, v in dict.iteritems():
            if isinstance(v, Column):
                v.modelName = k

        return type.__new__(cls, name, bases, dict)


class Model(object):

    __metaclass__ = ModelType


    _table = None
    _schema = None
    _columns = dict()
    _primary = ID

    id = Key(ID)

    def __init__(self, **kwargs):


        self.__dict__.update(kwargs)

        # If the object doesn't have a primary value, put none
        if not self.__dict__.has_key(self.primary()):
            self.setPrimary(None)

    @classmethod
    def all(cls):

        return Filter(cls.primary(), Condition.ALL)

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
        Return a map of the columns of the class by their *client* names, not class names
        :return:
        """


        if not cls._columns:
            columns = {}
            primary = None

            for  k,v in cls.__dict__.iteritems():
                if  isinstance(v, Column):
                    columns[v.name] = v

                    if v.primary:
                        primary = k

            if primary is not None:
                cls._primary = primary
            else:
                columns[Model._primary] = getattr(Model, Model._primary)

            if cls != Model:
                cls._columns = columns
                cls._columns.update(Model.columns())
            return columns
        else:
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
        obj.setPrimary(entity.id)

        for k,v in entity.properties.iteritems():

            if  k == cls.primary():
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

        cols = self.columns()
        primary = self.primary()
        pcol = cols[primary]

        ent = Entity(pcol.encode(getattr(self, primary)))

        for k, col in cols.iteritems():

            if k == primary:

                continue

            if not self.__dict__.has_key(col.modelName):
                if col.required:
                    raise ColumnValueError("Required column %s not set in %s" %(col.name, self._table))

                if not col.hasDefault:
                    continue

            data = self.__dict__.get(col.modelName) or col.default()
            #print k, data
            #col.validateChoices(data)
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

        return '%s<%s> %s' %(self._table, getattr(self, self._primary), self.__dict__)


    def setPrimary(self, val):
        """
        Set the value of the primary key
        :param val: the value
        :return:
        """
        pcol = self.columns()[self.primary()]

        setattr(self, self.primary(), pcol.decode(val))

