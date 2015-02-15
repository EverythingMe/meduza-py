import json

__author__ = 'dvirsky'



from queries import *
from client import *
from model import Model, Key, Text


__client = None


class MeduzaError(Exception):
    pass

def init(host='localhost', port=9977, timeout=0.1):

    global __client

    __client = RedisClient(host, port, timeout)



def select(model, *filters, **kwargs):


    q = queries.GetQuery(model._table, filters=filters,
                         properties=kwargs.get('properties', tuple()),
                         order=kwargs.get('order', None),
                         paging=kwargs.get('paging', None))

    res = __client.do(q)

    if res.error is not None:
        raise MeduzaError(res.error)

    return res.load(model)


def get(model, *ids):


    q = queries.GetQuery(model._table).filter(model._primary, Condition.IN, *ids)

    res = __client.do(q)

    if res.error is not None:
        raise MeduzaError(res.error)

    return res.load(model)

def put(*objects):

    #TODO: verify all objects are of the same model

    q = queries.PutQuery(objects[0]._table)
    for obj in objects:
        q.add(obj.encode())

    res = __client.do(q)

    if res.error is not None:
        raise MeduzaError("Error putting objects: %s", res.error)

    for id in res.ids:
        obj.setPrimary(id)
    return res.ids


class User(Model("Users")):

    id = Key("id", )
    name = Text("name")
    email = Text("email")





if __name__ == '__main__':

    init('localhost', 9977)

    #print get(User, User.name == "dvir", paging=Paging(0,10))

    u = User(name="dvir", email="dvir@everything.me", zmail="fo")
    print u

    ids = put(u)
    print ids
    # print get(User, u.id)



    # print User.columns()
    #
    # u = User(id="223423", name="dvir", email="dvir@everything.me", zmail="fo")
    # print u
    # e = u.encode()
    #
    # print e
    #
    # u2 = User.decode(e)
    # print u2
    #
    # q = User.get().filter(User.name == "dvir").limit(10)
    # client = RedisClient('localhost', 9977)
    #
    # r = client.do(q).load(User)
    # print r



    # print _("name") == "foo"
    # print _("name") > 24323
    # print _("someval") & [1,2,4]
    #
    #
    #
    #
    # pq = PutQuery("Users")
    # pq.add(Entity("", name="Dvir", email="foofoo@booboo.com", registrationTime = datetime.datetime.utcnow()))
    #
    #
    # pr = client.do(pq)
    # print pr.ids
    #
    #
    #
    # #
    # q = client.get(User).filter(User.name.IN("dvir", "joey")).limit(10)
    # filter_by(name="bla")
    # #
    # #
    # #
    # #
    # resp = client.do(q)
    # print "Error: ", resp.error
    #
    # for ent in resp.entities:
    #     print ent
