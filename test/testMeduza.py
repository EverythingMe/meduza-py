from contextlib import contextmanager
import logging

__author__ = 'dvirsky'

try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json

import meduza
from meduza.columns import Text, Timestamp, Key, Set, Float, Int, List
from meduza.queries import Ordering
from unittest import TestCase
import requests

schema = """
tables:
    TestUsers:
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
        indexes:
            -   type: compound
                columns: [name,email]
"""

class User(meduza.Model):

    _table = "TestUsers"

    name = Text("name", required=True)
    email = Text("email", default='')

    registrationTime = Timestamp("registrationTime", default=Timestamp.now)
    groups = Set("groups", type=Text())


import subprocess
import time
import os
import sys

class MeduzaTest(TestCase):


    @classmethod
    def runMeduza(cls):

        meduzad = os.getenv('MEDUZA_BIN', 'meduzad')

        cls.mdz = subprocess.Popen((meduzad, '-test', '-port=9957'), stdout=sys.stdout)
        time.sleep(0.5)


    @classmethod
    def installSchema(cls):

        #curl -i -X POST  -H "Content-Type: text/yaml" http://localhost:9966/deploy?name=foo --data-binary "@./evme.schema.yaml"
        deployUrl = "http://localhost:9966/deploy?name=testung"

        res = requests.post(deployUrl, schema, headers={"Content-Type": "text/yaml"})
        if res.status_code != 200 or res.content != 'OK':
            cls.fail("Failed deploying schema: %s" % res)



    @classmethod
    def setUpClass(cls):

        cls.runMeduza()

        cls.installSchema()
        meduza.init("localhost", 9957)


    @classmethod
    def tearDownClass(cls):
        cls.mdz.terminate()

    def setUp(self):
        self.users = []
        self.ids =[]
        for i in xrange(10):

            u = User(name = "user %d" % i, email = "user%d@domain.com" % i, groups = set(("g%d" % x for x in xrange(i, i+3))))
            self.users.append(u)

        self.ids = meduza.put(*self.users)

    def tearDown(self):
        pass
        meduza.delete(User, User.id.IN(*[u.id for u in self.users]))


    def testPut(self):

        self.assertEqual(len(self.ids), len(self.users))
        for i, u in enumerate(self.users):
            self.assertNotEqual(u.id, "")
            self.assertEqual(self.ids[i],u.id)



    def testGet(self):
        users = meduza.get(User, *[u.id for u in self.users])

        self.assertEqual(len(users), len(self.users))

        for i, u in enumerate(self.users):
            self.assertEquals(u.id, users[i].id)
            self.assertEquals(u.name, users[i].name)
            self.assertEquals(u.email, users[i].email)
            self.assertEquals(u.groups, users[i].groups)


    def testSelect(self):

        u = self.users[0]
        users = meduza.select(User, User.name==u.name,  User.email==u.email)

        self.assertEqual(len(users), 1)

        u2 = users[0]

        self.assertEquals(u.id, u2.id)
        self.assertEquals(u.name, u2.name)
        self.assertEquals(u.email, u2.email)
        self.assertEquals(u.groups, u2.groups)

        # select an impossible value
        users = meduza.select(User, User.name==u.name,  User.email=="not user 1's email")
        self.assertEquals(len(users), 0)


        # select by a partial key
        users = meduza.select(User, User.name==u.name)
        self.assertEqual(len(users), 1)

        u2 = users[0]

        self.assertEquals(u.id, u2.id)
        self.assertEquals(u.name, u2.name)
        self.assertEquals(u.email, u2.email)
        self.assertEquals(u.groups, u2.groups)


        #test select ALL
        users = meduza.select(User, User.all(), order=Ordering.asc('id'))
        self.assertEqual(len(users), len(self.users))

        for i, u in enumerate(self.users):
            self.assertEquals(u.id, users[i].id)
            self.assertEquals(u.name, users[i].name)
            self.assertEquals(u.email, users[i].email)
            self.assertEquals(u.groups, users[i].groups)


    def testDelete(self):

        n = meduza.delete(User, User.id.IN(*[u.id for u in self.users]))
        self.assertEquals(n, len(self.users))
        users =  meduza.select(User, User.id.IN(*[u.id for u in self.users]))

        self.assertEqual(0, len(users))


    def testUpdate(self):

        u = self.users[0]
        n = meduza.update(User, User.name==u.name,  User.email==u.email, name="baba")

        self.assertEquals(n, 1)

        users = meduza.select(User, User.name=="baba",  User.email==u.email)
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, u.id)

        users = meduza.select(User, User.name==u.name,  User.email==u.email)
        self.assertEqual(len(users), 0)

