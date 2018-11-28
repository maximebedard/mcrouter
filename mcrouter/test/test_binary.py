# Copyright (c) 2016-present, Facebook, Inc.
#
# This source code is licensed under the MIT license found in the LICENSE
# file in the root directory of this source tree.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import time

from mcrouter.test.MCProcess import Memcached
from mcrouter.test.MCProtocol import MCBinaryProtocol, MCAsciiProtocol
from mcrouter.test.McrouterTestCase import McrouterTestCase

class TestMemcachedBinaryProtocolBasic(McrouterTestCase):
    config = './mcrouter/test/mcrouter_test_basic_1_1_1.json'
    null_route_config = './mcrouter/test/test_nullroute.json'
    extra_args = []

    def setUp(self):
        # The order here corresponds to the order of hosts in the .json
        self.mc = self.add_server(self.make_memcached())

    def get_mcrouter(self, additional_args=[]):
        return self.add_mcrouter(
                self.config, extra_args=self.extra_args + additional_args, protocol=MCBinaryProtocol())

    def test_basic_cas(self):
	mcr = self.get_mcrouter()
	self.assertIsNone(mcr.cas('key', 'value', 1))
	self.assertIsNone(mcr.gets('key'))
	self.assertTrue(mcr.add('key', 'value'))
	ret = mcr.gets('key')
	self.assertIsNotNone(ret)
	old_cas = ret['cas']
	self.assertEqual(ret['value'], 'value')
	self.assertTrue(mcr.cas('key', 'value2', ret["cas"]))
	ret = mcr.gets('key')
	self.assertEqual(ret['value'], 'value2')
	self.assertNotEqual(old_cas, ret['cas'])
	self.assertTrue(mcr.set('key', 'value2'))
	self.assertFalse(mcr.cas('key', 'value3', ret['cas']))
	self.assertEqual(mcr.gets('key')['value'], 'value2')
    
    def test_basic_touch(self):
        mcr = self.get_mcrouter()

       # # positive
        self.assertTrue(mcr.set('key', 'value', exptime=0))
       # self.assertEqual(mcr.get('key'), 'value')
       # self.assertEqual(mcr.touch('key', 20), "TOUCHED")
       # self.assertEqual(mcr.get('key'), 'value')

       # # negative
       # self.assertEqual(mcr.touch('fake_key', 20), "NOT_FOUND")
       # self.assertIsNone(mcr.get('fake_key'))

       # # negative exptime
       # self.assertTrue(mcr.set('key1', 'value', exptime=10))
       # self.assertEqual(mcr.get('key1'), 'value')
       # self.assertEqual(mcr.touch('key1', -20), "TOUCHED")
       # self.assertIsNone(mcr.get('key1'))

       # # past
       # self.assertTrue(mcr.set('key2', 'value', exptime=10))
       # self.assertEqual(mcr.get('key'), 'value')
       # self.assertEqual(mcr.touch('key', 1432250000), "TOUCHED")
       # self.assertIsNone(mcr.get('key'))
# class TestMemcachedBinaryProtocol(McrouterTestCase):
#    def setUp(self):
#        self.mc1 = self.add_server(Memcached())
#    	self.mc1.protocol = MCBinaryProtocol()
# 
#  # def test_get(self):
#  #   self.assertIsNone(self.mc1.get("foo"))
# 
#  # def test_gets(self):
#  #   self.assertIsEmpty(self.mc1.gets("foo", "bar"))
# 
#  def test_set(self):
#      self.assertIsNone(self.mc1.set("foo", "bar"))
#    self.mc1.dump()
#     self.assertEqual(self.mc1.get("foo"), "bar")
# 
#  # def test_add(self):
#  #   self.assertIsNone(self.mc1.add("bar", "baz"))
#  #   self.assertEqual(self.mc1.get("bar"), "baz")

# class TestPoolServerErrorsBinary(McrouterTestCase):
#     config = './mcrouter/test/test_pool_server_errors.json'

#     def setUp(self):
#         self.mc1 = self.add_server(Memcached())
#         # mc2 is ErrorRoute
#         self.mc3 = self.add_server(Memcached())

#     def test_pool_server_errors(self):
#         mcr = self.add_mcrouter(self.config, '/a/a/')
#         self.assertIsNone(mcr.get('test'))

#         stats = mcr.stats('servers')
#         self.assertEqual(2, len(stats))

#         self.assertTrue(mcr.set('/b/b/abc', 'valueA'))
#         self.assertEqual(self.mc1.get('abc'), 'valueA')

#         self.assertFalse(mcr.set('/b/b/a', 'valueB'))

#         self.assertTrue(mcr.set('/b/b/ab', 'valueC'))
#         self.assertEqual(self.mc3.get('ab'), 'valueC')
