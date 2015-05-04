"""
Query and response objects for the Meduza protocol.

** * * WARNING * * **

Note that these objects are encoded to BSON and the server decodes them and expects them to have certain fields
in a very specific naming. So DO NOT ALTER these objects - not field names, not object names, not removing fields -
without knowing what you're doing and modifying the server code accordingly.

** * * /WARNING * * **
"""



class Condition(object):
    """
    Selection condition constants for use when constructing filters
    """

    IN = "IN"
    EQ = "="
    GT = ">"
    LT = "<"
    ALL = "ALL"



class Entity(object):
    """
    An entity represents a stored object in it's raw, schemaless form.
    You can work with entities directly, but for most cases you're better off mapping them to real objects
    """

    ID = 'Id'

    def __init__(self, _key, **properties):

        self.id = _key
        self.properties = properties

    def __repr__(self):

        return 'Entity<%s>: %s' % (self.id, self.properties)



class Response(object):
    """
    The base class for all responses. Includes the query processing time on the server, and the error returned from it
    """
    def __init__(self, **kwargs):
        self.error = kwargs.get('error', None)
        self.time = kwargs.get('time', )





class Filter(object):
    """
    A query selection filter, used to select objects for laoding or deletion
    """

    def __init__(self, property, op, *values):
        self.property = property
        self.op = op
        self.values = values

    def __repr__(self):
        return "Filter{%s %s %s}" % (self.property, self.op, self.values)

    def __and__(self, other):

        if isinstance(other, (list, tuple)):
            return (self,) + other

        return tuple((self, other))

class Ordering(object):
    """
    Representing the sort order of a query
    """
    ASC = 'ASC'
    DESC = 'DESC'
    def __init__(self, by, mode=ASC):
        self.by = by
        self.asc = mode == Ordering.ASC

    @classmethod
    def asc(cls, by):
        return Ordering(by, cls.ASC)

    @classmethod
    def desc(cls, by):
        return Ordering(by, cls.DESC)

class Paging(object):
    """
    Paging represents the paging limitations of a selection query
    """
    def __init__(self, offset=0, limit=100):
        self.offset = offset
        self.limit = limit



def Filters(*filters):

    ret = {}
    for flt in filters:
        ret[flt.property] = flt

    return ret

class GetQuery(object):
    """
    GetQuery encodes the parameters to get objects from the server
    """
    def __init__(self, table, properties = tuple(), filters=tuple(), order=None, paging=None):

        self.table = table
        self.properties = list(properties)
        self.filters = Filters(*filters)
        self.order = order
        self.paging =paging or Paging()

    def filter(self, prop, condition, *values):
        """
        Adds a filter (WHERE <prop> <condition> <values...>) to the query
        :param prop: property name for filtering
        :param condition: IN/=/LT/GT/...
        :param values: filtered values
        :return: the query object itself for builder-style syntax
        """

        self.filters[prop] = Filter(prop, condition, *values)

        return self



    def all(self):
        """
        Add a special filter to page on all ids with a certain paging
        :return:
        """

        self.filters[Entity.ID] = Condition.ALL



    def limit(self, limit):
        """
        Set limit on the first N records for this query. We assume offset 0
        :return: the query object itself for builder-style syntax
        """

        self.paging = Paging(0, limit)
        return self

    def page(self, offset, limit):
        """
        Set more complex paging offsets
        :param offset: where to begin fetching objects at
        :param limit: number of object to fetch
        :return: the query object itself for builder-style syntax
        """
        if offset >= limit or offset < 0 or limit <= 0:
            raise ValueError("Invalid offset/limit: {}-{}".format(offset,limit))

        self.paging= Paging(offset,limit)
        return self


class GetResponse(Response):
    """
    GetResponse is a response to a Get query, with the selected entities embedded in it
    """
    def __init__(self, **kwargs):
        Response.__init__(self, **kwargs['Response'])

        self.entities = [Entity(e['id'], **e['properties']) for e in kwargs.get('entities', [])]
        self.total = kwargs.get('total', 0)


    def load(self, model):


        ret = []
        for e in self.entities:

            ret.append(model.decode(e))

        return ret

    def loadOne(self, model):

        if len(self.entities) == 0:
            return None

        return model.decode(self.entities[0])


class PutQuery(object):
    """
    PutQuery is a batch insert/update query, pushing multiple objects at once.
    It's the fastest way to create multiple objects at once, and can create hundreds of objects in a single go
    """
    def __init__(self, table, *entities):

        self.table = table
        self.entities = list(entities)

    def add(self, entity):
        """
        Add an entity to be sent to the server
        :param entity: an entity object. ** It can (and should) be with an empty id if you're inserting **
        :return: the query object itself for builder-style syntax
        """
        self.entities.append(entity)
        return self


class PutResponse(Response):
    """
    PutResponse represents the response from a PUT query.
    It holds the ids of the put objects, whether they were new inserts or just updates/
    """
    def __init__(self, **kwargs):

        Response.__init__(self, **kwargs['Response'])
        self.ids = kwargs.get('ids', [])


    def __repr__(self):

        return 'PutResponse%s' % self.__dict__


class DelQuery(object):
    """
    DelQuery sets filters telling the server what objects to delete. It returns the number of objects deleted
    """
    def __init__(self, table, *filters):

        self.table = table
        self.filters = Filters(*filters)

    def filter(self, prop, op, *values):

        self.filters[prop] = Filter(prop, op, *values)
        return self


class DelResponse(Response):

    def __init__(self, **kwargs):

        Response.__init__(self, **kwargs.get('Response', {}))
        self.num = kwargs.get('num', 0)


class Change(object):

    Set        = "SET"
    Del        = "DEL"
    Increment  = "INCR"
    SetAdd     = "SADD"
    SetDel     = "SDEL"
    MapSet     = "MSET"
    MapDel     = "MDEL"


    def __init__(self, property, op, value):
        if op not in (self.Set, self.Increment):
            raise ValueError("op %s not supported", op)

        self.property = property
        self.op = op
        self.value = value

    @classmethod
    def set(cls, prop, val):
        """
        Create a SET change
        """

        return Change(prop, cls.Set, val)


class UpdateQuery(object):
    """
    DelQuery sets filters telling the server what objects to delete. It returns the number of objects deleted
    """
    def __init__(self, table,  filters, *changes):

        self.table = table
        self.filters = Filters(*filters)
        self.changes = changes

    def filter(self, prop, operator, *values):
        """
        Add an extra selection filter for what objects to update
        :param prop: the name of the filtered property
        :param operator: the filter operator (equals, gt, in, etc)
        :param values: the values for selection (e.g. "id" "in" 1,2,34)
        :return: the update query itself
        """

        self.filters[prop] = Filter(prop, operator, *values)
        return self

    def set(self, prop, val):
        """
        Add another SET change of a property to the query
        :param prop: the name of the changed property
        :param val: the changed value
        :return: the update query object itself
        """
        self.changes.append(Change.set(prop, val))
        return self

class UpdateResponse(Response):

    def __init__(self, **kwargs):

        Response.__init__(self, **kwargs.get('Response', {}))
        self.num = kwargs.get('num', 0)


class PingQuery(object):

    def __init__(self):
        pass


class PingResponse(Response):
    pass





