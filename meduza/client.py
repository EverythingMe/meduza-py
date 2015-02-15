__author__ = 'dvirsky'

import logging
import types
import redis
import bson
from bson.errors import BSONError
import time
import datetime

import queries




class Message(object):
    """
    A message represent a single protocol message passed between the client and server
    """

    GET = "GET"
    GET_RESPONSE = "RGET"
    PUT = "PUT"
    PUT_RESPONSE = "RPUT"
    SET = "SET"
    SET_RESPONSE = "RSET"
    DELETE = "DEL"
    DELETE_RESPONSE = "RDEL"


    def __init__(self, msgType, data):

        self.type = msgType
        self.body = data





class BsonProtocol(object):
    """
    BSON serializatino and deserialization between client and server
    """


    def decodeMessage(self, msg):
        """
        Take a binary encoded transport message and decode it into an internal response type
        :param msg: a binary Message
        :return: the decoded object on success. We raise an exception if we can't decode the response
        """

        try:
            res = bson.decode_all(msg.body)
        except BSONError as e:
            logging.exception("Could not decode message body")
            raise e

        if msg.type == Message.GET_RESPONSE:
            return queries.GetResponse(**res[0])
        elif msg.type == Message.PUT_RESPONSE:
            return queries.PutResponse(**res[0])

        raise ValueError("Unkonwn message type %s", msg.type)

    def encodeMessage(self, data):
        """
        take a known protocol passable object (query, basically) and encode it into a BSON message for the protocol
        :param data:
        :return:
        """

        try:
            d = self.dictify(data)
        except Exception as e:
            logging.exception("Could not dictify object %s", data)
            raise e

        body = None
        try:
            body = bson.BSON.encode(d)
        except (BSONError, ValueError) as e:
            logging.exception("Could not encode object %s to json", data)
            raise e

        t = self._messageType(data)
        if t is None:
            raise ValueError("Cannot encode object %s as a network message" % type(data))

        return Message(t,  body)


    def _messageType(self, data):
        """
        return the protocol type of an encoded message based on the underlying object type
        :param data: a known protocol object
        :return: the message type string if the object is known, else None
        """

        if isinstance(data, queries.GetQuery):
            return Message.GET
        elif isinstance(data, queries.PutQuery):
            return Message.PUT

        logging.warn("Unknown message type %s", type(data))
        return None





    primitives = (str, unicode, int, float, bool, types.NoneType, long, datetime.datetime)

    @classmethod
    def dictify(cls, obj):
        """
        Take an object and recursively translate its __dict__'s members to dicts,
        returning a pure dict/list/primitive view of this object, so it can be serialized to BSON
        :param obj:
        :return:
        """

        if isinstance(obj, cls.primitives):
            return obj

        if isinstance(obj, (list, tuple, set, frozenset)):
            return [cls.dictify(e) for e in obj]

        elif isinstance(obj, dict):
            return {k: cls.dictify(dv)  for k,dv in obj.iteritems()}

        return {k: cls.dictify(v) for k,v in obj.__dict__.iteritems()}





class RedisTransport(object):
    """
    Transport represents a single server connection that can read and write messages.
    Currently we have just one transport - redis transport
    """

    def __init__(self, pool):
        """
        We initialize the transport with a redis connection pool from which it takes a connection
        """

        self._conn = pool.get_connection("GET")

        assert(isinstance(self._conn, redis.Connection))


    def sendMessage(self, msg):
        """
        Send a single serialized message to the server.
        :param msg: a serialized message
        """
        assert(isinstance(msg, Message))

        self._conn.connect()
        self._conn.send_command(msg.type, msg.body)

    def receiveMessage(self):
        """
        Receive a single serialized message from the server.
        :return: a serialized message
        """

        msgType, body = self._conn.read_response()

        return Message(msgType, body)



class RedisClient(object):
    """
    A client connects to the server using redis' RESP protocol,
    sends commands and receives responses, taking care of serialization.

    You can use a single redis client per app, as it is thread safe and uses a redis connection pool internally.
    """

    def __init__(self, host='localhost', port=9977, timeout=0.1):

        self._pool = redis.ConnectionPool(host=host,port=port, socket_timeout=timeout)
        self._proto = BsonProtocol()



    def send(self, query, transport):
        """
        Send a query to the server (without receiving the response)
        * Do not use this method unless for pipelining, use do() instead for single queries *
        :param query: a query object
        :param transport: a redis transport.
        :return:
        """

        msg = self._proto.encodeMessage(query)
        transport.sendMessage(msg)

    def receive(self, transport):
        """
        Received a response from the server and deserialize it into a response object
        * Do not use this method unless for pipelining, use do() instead for single queries *
        :param transport: a redis transport.
        :return:
        """

        msg = transport.receiveMessage()

        return self._proto.decodeMessage(msg)


    def do(self, query):
        """
        Send a query to the server and receive its response
        :param query: a query object
        :return: a response object
        """


        conn = RedisTransport(self._pool)

        self.send(query, conn)

        return self.receive(conn)












