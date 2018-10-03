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
from mcrouter.test.MCProtocol import MCBinaryProtocol
from mcrouter.test.McrouterTestCase import McrouterTestCase

class TestMemcachedBinaryProtocol(McrouterTestCase):
  def setUp(self):
    self.mc1 = self.add_server(Memcached())
    self.mc1.protocol = MCBinaryProtocol()

  # def test_get(self):
  #   self.assertIsNone(self.mc1.get("foo"))

  # def test_gets(self):
  #   self.assertIsEmpty(self.mc1.gets("foo", "bar"))

  def test_set(self):
    self.assertIsNone(self.mc1.set("foo", "bar"))
#    self.assertEqual(self.mc1.get("foo"), "bar")

  # def test_add(self):
  #   self.assertIsNone(self.mc1.add("bar", "baz"))
  #   self.assertEqual(self.mc1.get("bar"), "baz")

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
