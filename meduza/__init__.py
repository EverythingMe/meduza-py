from contextlib import contextmanager

from meduza.queries import *
from meduza.client import *
from meduza.model import Model
from meduza.columns import Key, Text, Timestamp, Set
from meduza.errors import MeduzaError, ModelError, RequestError


__author__ = 'dvirsky'


def customConnector(host, port, timeout=0.5):

    @contextmanager
    def connector():
        yield RedisClient(host=host, port=port,timeout=timeout)

    return connector

@contextmanager
def defaultConnector():
    yield RedisClient()



def ping(client):
        q = PingQuery()
        res = client.do(q)
        return res.error is None

class Session(object):

    def __init__(self, masterConnector = defaultConnector, slaveConnector = defaultConnector):

        self._master = masterConnector
        self._slave = slaveConnector

    def select(self, model, *filters, **kwargs):
        """
        Select objects based on secondary indexes. The model class is used to construct object instances
        :param model: a model class to create instances from
        :param filters: a list of filters
        :param kwargs: extra parameters:
            * properties - a list of properties to get (NOT SUPPORTED SERVER SIDE YET)
            * order - an ordering object (order by ? asc/desc)
            * paging - start/offset
            * limit - same as paging but start=0
        :return: a list of objects generated from the model class
        """
        if 'paging' in kwargs:
            paging = kwargs['paging']
        elif 'limit' in kwargs:
            paging = Paging(0, kwargs['limit'])
        else:
            paging = None

        q = queries.GetQuery(model.tableName(), filters=filters,
                             properties=kwargs.get('properties', tuple()),
                             order=kwargs.get('order', None),
                             paging=paging)

        with self._slave() as client:
            res = client.do(q)

        if res.error is not None:
            raise RequestError(res.error)

        return res.load(model)


    def get(self, model, *ids):
        """
        Get objects by id(s), automatically generating instances of the model class
        :param model: a model class used to generate objects from the returned entities
        :param ids: a set of id strings
        :return: a list of model object instances
        """
        for id in ids:
            if not isinstance(id, basestring):
                raise MeduzaError("Invalid id type: %s", type(id))

        q = queries.GetQuery(model.tableName()).filter(model.primary(), Condition.IN, *ids)

        with self._slave() as client:
            res = client.do(q)

        if res.error is not None:
            raise RequestError(res.error)

        return res.load(model)


    def put(self, *objects):
        """
        Put a bunch of model objects into meduza.
        If the objects have an id set, it is respected by the server. If not, a new id is generated and the objects
        are filled with their respective ids automatically.

        NOTE: All objects must be of the same model class
        :param objects: a list of model objects of the same class
        :return: the ids resulting from putting the objects into meduza
        """
        q = queries.PutQuery(objects[0].tableName())

        cls = None
        for obj in objects:
            if cls is None:
                cls = obj.__class__

            if obj.__class__ != cls:
                raise MeduzaError("All objects in a PUT call must be of the same class")

            if not isinstance(obj, Model):
                raise ModelError("Non model object found")

            q.add(obj.encode())

        with self._master() as client:
            res = client.do(q)

        if res.error is not None:
            raise RequestError("Error putting objects: %s", res.error)

        for i, id in enumerate(res.ids):

            objects[i].setPrimary(id)

        return res.ids


    def delete(self, model, *filters):
        """
        Delete from a model, based on a series of filters
        :param model: a model class. This is just used to extract the table name
        :param filters: a list of filters
        :return: the number of entities deleted
        """
        q = queries.DelQuery(model.tableName(), *filters)

        with self._master() as client:
            res = client.do(q)

        if res.error is not None:
            raise RequestError("Error deleting objects: %s", res.error)

        return res.num


    def update(self, model, *filters, **changes):
        """
        Update performs an UPDATE query on the model
        :param model: a model class we use to take the table name from
        :param filters: a list of selection filters to determine which entities to update
        :param changes: a set of key=value changes to set. e.g. name="Foo"
        :return: the number of updated entities
        """
        q = queries.UpdateQuery(model.tableName(), *filters, **changes)

        with self._master() as client:
            res = client.do(q)

        if res.error is not None:
            raise RequestError("Error deleting objects: %s", res.error)

        return res.num


_defaultSession = None



def setup(masterConnector = defaultConnector, slaveConnector = defaultConnector):
    """
    initialize or reconfigure the global meduza client
    :param masterProvider: a context manager which yields a client
    :param slaveProvider: a context manager which yields a client
    """
    logging.info("Setting up meduza client bandit")

    global _defaultSession
    _defaultSession = Session(masterConnector, slaveConnector)


def select(model, *filters, **kwargs):
    return _defaultSession.select(model, *filters, **kwargs)

def get(self, model, *ids):
    """
    Get objects by id(s) from the default session, automatically generating instances of the model class
    :param model: a model class used to generate objects from the returned entities
    :param ids: a set of id strings
    :return: a list of model object instances
    """
    return _defaultSession.get(self, model, *ids)

def put(self, *objects):
    """
    Put a bunch of model objects into meduza using the Default Session
    If the objects have an id set, it is respected by the server. If not, a new id is generated and the objects
    are filled with their respective ids automatically.

    NOTE: All objects must be of the same model class
    :param objects: a list of model objects of the same class
    :return: the ids resulting from putting the objects into meduza
    """
    return _defaultSession.put(self, *objects)

def delete(self, model, *filters):
    """
    Delete from a model, based on a series of filters using the Default Session
    :param model: a model class. This is just used to extract the table name
    :param filters: a list of filters
    :return: the number of entities deleted
    """
    return _defaultSession.delete(self, model, *filters)

def update(self, model, *filters, **changes):
    """
    Update performs an UPDATE query on the model
    :param model: a model class we use to take the table name from
    :param filters: a list of selection filters to determine which entities to update
    :param changes: a set of key=value changes to set. e.g. name="Foo"
    :return: the number of updated entities
    """
    return _defaultSession.update(self, model, *filters, **changes)