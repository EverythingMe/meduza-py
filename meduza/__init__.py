from contextlib import contextmanager
import socket
from redis import RedisError
import os
__author__ = 'dvirsky'

from meduza.queries import *
from meduza.client import *
from meduza.model import Model
from meduza.columns import Key, Text, Timestamp, Set
from meduza.errors import MeduzaError, ModelError, RequestError

try:
    from bandit_lb import LoadBalancer
except ImportError:
    #this is used because we import meduza for testing purposes without actually using the client.
    if not os.getenv("TEST", False):
        raise

#__client = None

__masterLb = None
__slaveLb = None

__timeout = 0.5


def __connect(host, port):

    return RedisClient(host, port, __timeout)

def __ping(client):

    q= PingQuery()
    res = client.do(q)
    if res.error is None:
        return True
    return False

@contextmanager
def _slaveConn():
    client = __slaveLb.getConnection()
    yield client
    __slaveLb.repoolConnection(client)

@contextmanager
def _masterConn():
    client = __masterLb.getConnection()
    yield client
    __masterLb.repoolConnection(client)


def init(master, slaves, timeout=0.5):
    """
    init and configure the meduza client
    :param host:
    :param port:
    :param timeout:
    :return:
    """
    logging.info("Initializing meduza client bandit")
    global __masterLb, __slaveLb
    global __timeout
    __timeout = timeout

    __masterLb = LoadBalancer(connectionInitCallback=__connect, name='MeduzaMaster',
                           monitoredExceptions=(socket.error, RedisError),
                           servers=master,
                           pingCallback=__ping,
                           pingInterval=2.0, connectionTTL=50, maxRetries=1, minHealthRatio=0.1)

    __slaveLb = LoadBalancer(connectionInitCallback=__connect, name='MeduzaMaster',
                           monitoredExceptions=(socket.error, RedisError),
                           servers=slaves,
                           pingCallback=__ping,
                           pingInterval=2.0, connectionTTL=50, maxRetries=1, minHealthRatio=0.1)

    # # Bind to dynamic configuration notifications
    # dynamic_configuration.Manager.addChangeListener((config.geodesic_client, 'geodesic_servers'),
    #                                       lambda path, servers: __lb.reloadServers(servers))




def select(model, *filters, **kwargs):
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

    q = queries.GetQuery(model._table, filters=filters,
                         properties=kwargs.get('properties', tuple()),
                         order=kwargs.get('order', None),
                         paging=kwargs.get('paging', None) if kwargs.has_key('paging') else \
                                (Paging(0, kwargs.get('limit')) if kwargs.has_key('limit') else None))

    with _slaveConn() as client:
        res = client.do(q)

    if res.error is not None:
        raise RequestError(res.error)

    return res.load(model)


def get(model, *ids):
    """
    Get objects by id(s), automatically generating instances of the model class
    :param model: a model class used to generate objects from the returned entities
    :param ids: a set of id strings
    :return: a list of model object instances
    """


    for id in ids:
        if not isinstance(id, basestring):
            raise MeduzaError("Invalid id type: %s", type(id))

    q = queries.GetQuery(model._table).filter(model._primary, Condition.IN, *ids)

    with _slaveConn() as client:
        res = client.do(q)

    if res.error is not None:
        raise RequestError(res.error)

    return res.load(model)

def put(*objects):
    """
    Put a bunch of model objects into meduza.
    If the objects have an id set, it is respected by the server. If not, a new id is generated and the objects
    are filled with their respective ids automatically.

    NOTE: All objects must be of the same model class
    :param objects: a list of model objects of the same class
    :return: the ids resulting from putting the objects into meduza
    """

    q = queries.PutQuery(objects[0]._table)
    cls = None
    for obj in objects:
        if cls is None:
            cls = obj.__class__

        if obj.__class__ != cls:
            raise MeduzaError("All objects in a PUT call must be of the same class")

        if not isinstance(obj, Model):
            raise ModelError("Non model object found")

        q.add(obj.encode())

    with _masterConn() as client:
        res = client.do(q)


    if res.error is not None:
        raise RequestError("Error putting objects: %s", res.error)

    for i, id in enumerate(res.ids):

        objects[i].setPrimary(id)

    return res.ids


def delete(model, *filters):
    """
    Delete from a model, based on a series of filters
    :param model: a model class. This is just used to extract the table name
    :param filters: a list of filters
    :return: the number of entities deleted
    """
    q = queries.DelQuery(model._table, *filters)


    with _masterConn() as client:
        res = client.do(q)

    if res.error is not None:
        raise RequestError("Error deleting objects: %s", res.error)

    return res.num



def update(model, *filters, **changes):

    q = queries.UpdateQuery(model._table, *filters, **changes)

    with _masterConn() as client:
        res = client.do(q)

    if res.error is not None:
        raise RequestError("Error deleting objects: %s", res.error)

    return res.num
