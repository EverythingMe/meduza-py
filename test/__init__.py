from contextlib import contextmanager
import logging

__author__ = 'dvirsky'

import meduza
from meduza.columns import Text, Timestamp, Key, Set, Float, Int, List

class User(meduza.Model("Users")):

    name = Text("name", required=True)
    email = Text("email", default='')

    registrationTime = Timestamp("registrationTime", default=Timestamp.now)
    mySet = Set("mySet", type=Text())

    myList = List("myList", type=Text())


class MarketInfo(meduza.Model("MarketInfo")):
    """
    Model for the MarketInfo API

    Notes:
        not storing storeName since everything is currently originated from Google Play.
        not storing size due to missing data
    """


    packageId = Text('packageId', required=True)
    locale = Text('locale', required=True)

    score = Float('score')

    rank = Float('rank')
    installsLower = Int('installs')

    name = Text('name')
    description = Text('desc')

    price = Float('price')
    currency = Text('currency')

    # Locale to thumbnail URLs string, the string is CSV formatted
    thumbnailUrls = Set('thumb', type=Text )

    lastModified = Timestamp('lmtime', default=Timestamp.now)



try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json

import dtjson


def load(buf):
    return meduza.put(*buf)




def importMarketInfo(path):

    buf = []
    st = time.time()
    for n, line in enumerate(open(path)):

        obj = json.loads(line)

        lm = obj.get('lastModified')
        if lm:
            obj['lastModified'] = dtjson.decode_datetime(lm)

        inf = MarketInfo(**obj)
        buf.append(inf)

        if n > 0 and n %10 == 0:

            meduza.put(*buf)
            elapsed = time.time() - st
            print n, elapsed, n/elapsed
            buf = list()
            return








        #print obj


#TSTabs = 0
@contextmanager
def TimeSampler(actionDescription = '', func = None, minTimeFilterMS = 0, fmtArgs = ()):

    global TSTabs
    #TSTabs += 1

    st = time.time()
    yield
    et = time.time()
    duration =  1000*(et - st)
    if duration < minTimeFilterMS:
        return
    #TSTabs -= 1
    #(TSTabs * '\t')
    msg = 'Action %s took %.03fms' % (actionDescription if not fmtArgs else actionDescription % fmtArgs, 1000*(et - st))

    print msg

if __name__ == '__main__':

    meduza.init('localhost', 9977)

    #print get(User, User.name == "dvir", paging=Paging(0,10))
    import time
    import cProfile


    def test():
        users = []
        N = 1
        for i in xrange(N):
            users.append(User(name="user %d"%i, email="dvir@everything.me",
                              mySet={"foo", "bar", "baz"}, myList=["bag", "bang", "bong"]))

        st = time.time()

        ids = meduza.put(*users)
        et = time.time()
        print et-st, N/(et-st)

        print meduza.get(User, *ids)

    test()

#    (meduza.select(MarketInfo, MarketInfo.packageId=='com.cnn.news.app.en', MarketInfo.locale.IN('en_us', 'en_gb', 'it', 'es', )))
#    importMarketInfo('/home/dvirsky/marketInfo.json')

    #with TimeSampler('loading'):
    #    print (meduza.select(MarketInfo, MarketInfo.packageId=='com.cnn.news.app.en', MarketInfo.locale.IN('en_us',)))
    #test()
    #cProfile.run("test()")
    # for id in ids:
    #     print id
    # print u
    #
    # ids = put(u)
    # print ids
    # print get(User, u.id)
    #
    #
    # print select(User, User.name == "dvir")
    #
    # print delete(User, User.name == "dvir")




