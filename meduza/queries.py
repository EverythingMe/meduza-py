__author__ = 'dvirsky'



class Op(object):

    IN = "IN"
    EQ = "="
    GT = ">"
    LT = "<"


class Filter(object):

    def __init__(self, property, op, *values):
        self.Property = property
        self.Op = op
        self.Values = values

class Ordering(object):
    ASC = 'ASC'
    DESC = 'DESC'
    def __init__(self, by, asc):
        self.by = by
        self.asc = True if asc == self.ASC else False

class Paging(object):

    def __init__(self, offset=0, limit=10):
        self.Offset = offset
        self.Limit = limit


class GetQuery(object):

    def __init__(self, table, properties = tuple(), filters=tuple(), order=None, paging=None):

        self.Table = table
        self.Properties = list(properties)
        self.Filters = list(filters)
        self.Order = order
        self.Paging =paging or Paging()

    def filter(self, prop, op, *values):

        self.Filters.append(Filter(prop, op, *values))

        return self

    def limit(self, limit):

        self.Paging = Paging(0, limit)
        return self

    def page(self, offset, limit):

        if offset >= limit or offset < 0 or limit <= 0:
            raise ValueError("Invalid offset/limit: {}-{}".format(offset,limit))

        self.Paging= Paging(offset,limit)
        return self

class Response(object):

    def __init__(self, Error = None, Time = 0):

        self.error = Error
        self.time = Time


class PutQuery(object):

    def __init__(self, table, *entities):

        self.Table = table
        self.Entities = list(entities)

    def add(self, entity):

        self.Entities.append(entity)
        return self


class PutResponse(Response):

    def __init__(self, **kwargs):

        Response.__init__(self, kwargs.get('Error'), kwargs.get('Time'))
        self.ids = kwargs.get('Ids', [][:])


class Entity(object):

    def __init__(self, id, **properties):

        self.Id = id
        self.Properties = properties


    def __repr__(self):

        return 'Entity<%s>: %s' % (self.Id, self.Properties)


class DelQuery(object):

     def __init__(self, table, *filters):

        self.Table = table
        self.Filters = list(filters)

    def filter(self, prop, op, *values):

        self.Filters.append(Filter(prop, op, *values))
        return self


class DelResponse(object):


class GetResponse(Response):

    def __init__(self, **kwargs):

        Response.__init__(self, kwargs.get('Error'), kwargs.get('Time'))

        self.entities = [Entity(e['Id'], **e['Properties']) for e in kwargs.get('Entities', [])]
        self.total = kwargs.get('Total', 0)


