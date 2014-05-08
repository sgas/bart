# -*- coding: utf-8 -*-

import random
import unittest
from pwd import getpwuid

import sys
sys.path.append("..")
 
from bart import common

class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        pass

    def test_getSeconds(self):
        print "Testing getSeconds()"
        self.assertEqual(common.getSeconds("1:0"), 1*60+0, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("1:1:0"), 1*3600+1*60+0, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("1-1:1:0"), 1*86400+1*3600+1*60+0, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("2:3"), 2*60+3, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("1:2:3"), 1*3600+2*60+3, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("1-2:3:4"), 1*86400+2*3600+3*60+4, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("2:3.1"), 2*60+3, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("1:2:3.2"), 1*3600+2*60+3, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("4-3:2:1.33"), 4*86400+3*3600+2*60+1, "Wrong number of seconds..")
        self.assertEqual(common.getSeconds("Smurf"), -1, "Failed to detect error...")
        self.assertEqual(common.getSeconds("1:a:3"), -1, "Failed to detect error...")
        self.assertEqual(common.getSeconds(""), -1, "Failed to detect error...")
        try:
            self.assertEqual(common.getSeconds("-1:2:3"), -1, "Failed to detect error...")
        except:
            pass
        
    def test_datetimeFromIsoStr(self):
        print "Testing datetimeFromIsoStr"
        self.assertEqual(common.datetimeFromIsoStr("2012-01-02T08:09:10").__str__(), "2012-01-02 08:09:10", "Not Equal..")
        self.assertEqual(common.datetimeFromIsoStr("2010-10-12T18:19:20").__str__(), "2010-10-12 18:19:20", "Not Equal..")
        
if __name__ == '__main__':
    unittest.main()
