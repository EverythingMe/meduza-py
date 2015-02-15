import json

__author__ = 'dvirsky'



from queries import *
from client import *


if __name__ == '__main__':


    pq = PutQuery("Users")
    pq.add(Entity("", name="Dvir", email="foofoo@booboo.com", registrationTime = datetime.datetime.utcnow()))
    client = RedisClient('localhost', 9977)

    pr = client.do(pq)
    print pr.ids



    #
    q = GetQuery("Users").filter("name", Op.IN, "dvir").limit(10)
    #
    #
    #
    #
    resp = client.do(q)
    print "Error: ", resp.error

    for ent in resp.entities:
        print ent
