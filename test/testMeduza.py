import logging
import signal
import time

from meduza.testing import DisposableMeduza


__author__ = 'dvirsky'

try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json

import meduza
from meduza.columns import Text, Timestamp, Set, Int, Map
from meduza.queries import Ordering, PingQuery, Change
from unittest import TestCase


schema = """
schema: pytest
tables:
    Users:
        engines:
            - redis
        primary:
            type: compound
            columns: [email]
        columns:
            name:
                type: Text
            email:
                type: Text
            time:
                type: Timestamp
            groups:
                type: Set
            wat:
                type: Text
                clientName: fancySuperLongNameWatWat
            score:
                type: Int

            mapr:
                type: Map
                options:
                    subtype: Text
        indexes:
            -   type: compound
                columns: [name,email]
"""


class User(meduza.Model):
    _table = "Users"
    _schema = "pytest"

    name = Text("name", required=True)
    email = Text("email", default='')

    registrationTime = Timestamp("registrationTime", default=Timestamp.now)
    groups = Set("groups", type=Text())

    fancySuperLongNameWatWat = Text("wat")

    mapr = Map("mapr", type=Text())

    score = Int("score", default=0)


import os
import sys


class MeduzaE2ETestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mdz = DisposableMeduza(os.getenv('MEDUZA_BIN', os.path.join(os.path.dirname(__file__), '..', 'run_mdz_docker.sh')))
        cls.mdz.start()

        try:
            cls.mdz.installSchema(schema)
        except Exception:
            logging.exception("Failed installing schema")
            cls.tearDownClass()
            sys.exit(-1)

        provider = meduza.customConnector('localhost', cls.mdz.port)
        meduza.setup(provider, provider)

    @classmethod
    def tearDownClass(cls):
        if cls.mdz is not None:
            cls.mdz.stop()

    def setUp(self):
        self.users = []
        self.ids = []
        for i in xrange(20):
            u = User(name="user %02d" % i, email="user%02d@domain.com" % i,
                     groups=set(("g%d" % x for x in xrange(i, i + 3))),
                     fancySuperLongNameWatWat="watwat",
                     mapr={"foo": "bar"},
            )
            self.users.append(u)

        self.ids = meduza.put(*self.users)



    def tearDown(self):

        meduza.delete(User, User.id.any(*[u.id for u in self.users]))


    def testPut(self):

        self.assertEqual(len(self.ids), len(self.users))
        for i, u in enumerate(self.users):
            self.assertNotEqual(u.id, "")
            self.assertEqual(self.ids[i], u.id)

    def testExpire(self):

        u = User(name="expi mcxpire", email="Expiry@domain.com")
        ids = meduza.putExpiring(0.1, u)
        self.assertGreater(len(ids), 0)
        us = meduza.get(User,ids[0])
        self.assertGreater(len(us), 0)
        self.assertEqual(us[0].id, u.id)

        time.sleep(0.15)
        us = meduza.get(User, ids[0])
        self.assertEqual(len(us), 0)

        us = meduza.select(User, User.all(), limit=len(self.users))
        self.assertGreater(len(us), 0)

        meduza.update(User, [User.id.any(*[u.id for u in self.users])], _=Change.expire(0.1))
        time.sleep(0.15)
        us = meduza.select(User, User.all(), limit=len(self.users))
        self.assertEqual(len(us), 0)




    def testGet(self):
        users = meduza.get(User, *[u.id for u in self.users])

        self.assertEqual(len(users), len(self.users))

        for i, u in enumerate(self.users):
            self.assertEquals(u.id, users[i].id)
            self.assertEquals(u.name, users[i].name)
            self.assertEquals(u.email, users[i].email)
            self.assertEquals(u.groups, users[i].groups)
            self.assertTrue(bool(u.fancySuperLongNameWatWat))
            self.assertEqual(u.fancySuperLongNameWatWat, users[i].fancySuperLongNameWatWat)


    def testSelect(self):

        u = self.users[0]
        users = meduza.select(User, User.name.equals(u.name) & User.email.equals(u.email))

        self.assertEqual(len(users), 1)

        u2 = users[0]

        self.assertEquals(u.id, u2.id)
        self.assertEquals(u.name, u2.name)
        self.assertEquals(u.email, u2.email)
        self.assertEquals(u.groups, u2.groups)

        # select an impossible value
        users = meduza.select(User, User.name.equals(u.name) & (User.email == "not user 1's email"))
        self.assertEquals(len(users), 0)


        # select by a partial key
        users = meduza.select(User, User.name == u.name)
        self.assertEqual(len(users), 1)

        u2 = users[0]

        self.assertEquals(u.id, u2.id)
        self.assertEquals(u.name, u2.name)
        self.assertEquals(u.email, u2.email)
        self.assertEquals(u.groups, u2.groups)


        # test select ALL
        users = meduza.select(User, User.all(), order=Ordering.asc('id'), limit=len(self.users)+1)
        self.assertEqual(len(users), len(self.users))

        for i, u in enumerate(self.users):
            self.assertEquals(u.id, users[i].id)
            self.assertEquals(u.name, users[i].name)
            self.assertEquals(u.email, users[i].email)
            self.assertEquals(u.groups, users[i].groups)

    def testCount(self):

        n = meduza.count(User, User.all())
        self.assertEquals(n, len(self.users))

        n = meduza.count(User)
        self.assertEquals(n, len(self.users))

    def testDelete(self):

        n = meduza.delete(User, User.id.any(*[u.id for u in self.users]))
        self.assertEquals(n, len(self.users))
        users = meduza.get(User, *[u.id for u in self.users])

        self.assertEqual(0, len(users))


    def testModelMagic(self):

        u = self.users[0]


        # Make sure that if update includes an increment statement where the key is not the same as the
        # the column being updated (we only allow score=User.score+1
        with self.assertRaises(AssertionError):
            meduza.update(User, User.name.equals(u.name) & User.email.equals(u.email),
                          name=User.score + 3)

        print meduza.update(User, User.name.equals(u.name) & User.email.equals(u.email),
                          fancySuperLongNameWatWat=User.fancySuperLongNameWatWat + 3)

        u = User(name ="baba")
        # Test that missing object attributes that are columns do not return the column class
        with self.assertRaises(AttributeError):
            u.fancySuperLongNameWatWat


    def testUpdate(self):

        u = self.users[0]
        n = meduza.update(User, User.name.equals(u.name) & User.email.equals(u.email),
                          Change.delProperty("email"),
                          name="baba", score=User.score + 3)

        self.assertEquals(n, 1)
        users = meduza.select(User, (User.name == "baba") & (User.email == u.email))
        self.assertEqual(len(users), 0)

        users = meduza.select(User, (User.name == "baba") & (User.email == ""))
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, u.id)
        self.assertEqual(users[0].score, u.score + 3)

        users = meduza.select(User, (User.name == u.name) & (User.email == u.email))
        self.assertEqual(len(users), 0)


    def testPing(self):

        with meduza.customConnector('localhost', self.mdz.port)() as client:
            ret = client.do(PingQuery())
            self.assertIsNotNone(ret)

            self.assertIsNone(ret.error)


class ModelEncodeDecodeTestCase(TestCase):
    def testEncodeModel(self):
        u = User(name="user", email="user@domain.com",
                 groups=set(("g%d" % x for x in xrange(0, 3))),
                 fancySuperLongNameWatWat="watwat",
                 mapr={"foo": "bar"},
                 )

        entity = u.encode()
        self.assertEqual(entity.properties['wat'], u.fancySuperLongNameWatWat)
        u2 = User.decode(entity)
        self.assertEqual(u.__dict__, u2.__dict__)
