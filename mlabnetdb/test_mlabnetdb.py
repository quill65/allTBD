
import unittest
import os, sys, tempfile, gzip

import mlabnetdb
from mlabnetdb import *



class mmUtilsTests(unittest.TestCase):

    def test_ipv4mask(self):

        self.assertEqual(ipv4mask(24), 0xffffff00)
        self.assertEqual(ipv4mask(32), 0xffffffff)
        self.assertEqual(ipv4mask(1),  0x80000000)
        self.assertEqual(ipv4mask(0),  0x00000000)
        self.assertRaises(Exception, ipv4mask, 33)

    def test_ipv4FromString(self):

        self.assertEqual(ipv4FromString('1.2.3.4'), 0x1020304)
        self.assertEqual(ipv4FromString('192.168'), 0xc0a80000)
        self.assertRaises(ValueError, ipv4FromString, '1.299.3.4')
        self.assertRaises(ValueError, ipv4FromString, '1.29.z.4')



class mmDbOpsTests(unittest.TestCase):

    def setUp(self):
        mlabnetdb._init()

    def test_loads(self):

        blocks = ['1.0.216.1/22',  # - 1.0.219.255
                  '155.168.0/24',  # - 155.168.0.255
                  '88.0.215.0/24', # - 88.0.215.255
                  '4.20.230.0/23', # - 4.20.231.255
                  '5.196.64.0/19', # - 5.196.95.255
                  '12.65.0.0/16',  # - 12.65.255.255
                  '99.0.0.0/8',    # - 99.255.255.255
                  '192/2'          # - 255.255.255.255
                  ]

        for b in blocks:
            mlabnetdb._addRecord(mlabnetdb._bmaps, b, b)
        mlabnetdb._bmapLoaded = True

        self.assertEqual(len(mlabnetdb._bmaps[22]), 1)
        self.assertEqual(len(mlabnetdb._bmaps[24]), 2)
        self.assertEqual(len(mlabnetdb._bmaps[25]), 0)

        self.assertEqual(lookup('1.0.217.22'), '1.0.216.1/22')
        self.assertEqual(lookup('192.168.0.16'), '192/2')
        self.assertEqual(lookup('155.168.0.16'), '155.168.0/24')
        self.assertEqual(lookup('12.65.255.111'), '12.65.0.0/16')
        self.assertEqual(lookup('12.66.0.111'), None)


        b = '0.0.0.0/0'
        mlabnetdb._addRecord(mlabnetdb._bmaps, b, b)
        for ip in ['1.0.217.22']:
            self.assertEqual(lookup(ip), b)
        


    def test_basic_file_load(self):

        tmpDb = tempfile.NamedTemporaryFile(suffix='.csv')
        tmpDb.write(bytes(testData, 'UTF-8'))
        tmpDb.flush()
        mlabnetdb.maxMindDbFile = tmpDb.name

        self.assertFalse(mlabnetdb._bmapLoaded)
        lookup('1.2.3.4')
        self.assertTrue(mlabnetdb._bmapLoaded)
        self.assertEqual(mlabnetdb._records_loaded, 11)


    def test_basic_gzipfile_load(self):

        gzdata = gzip.compress(bytes(testData, 'UTF-8'))
        tmpDb = tempfile.NamedTemporaryFile(suffix='.csv.gz')
        tmpDb.write(gzdata)
        tmpDb.flush()
        mlabnetdb.maxMindDbFile = tmpDb.name

        lookup('1.2.3.4')
        self.assertEqual(mlabnetdb._records_loaded, 11)


    def test_testData_records(self):

        tmpDb = tempfile.NamedTemporaryFile(suffix='.csv')
        tmpDb.write(bytes(testData, 'UTF-8'))
        tmpDb.flush()
        mlabnetdb.maxMindDbFile = tmpDb.name

        self.assertEqual(lookup('4.4.4.4'), None)
        self.assertEqual(lookup('63.246.159.15')['autonomous_system_number'], 395374)

        self.assertEqual(lookup('1.0.1.10')['autonomous_system_number'], None)
        self.assertEqual(lookup('1.0.2.10')['autonomous_system_number'], None)


testData = """network,isp,organization,autonomous_system_number,autonomous_system_organization
1.0.0.0/24,"Xyzpd Pty","Xyzpd Pty",,
2.19.60.0/22,"Mlkjsd","Mlkjsd",20940,"xyz B.V."
2.19.64.0/20,"Mlkjsd","Mlkjsd",16625,"xyz, Inc."
1.0.1.0/24,"3elecom","3elecom",,
1.0.2.0/23,"3elecom","3elecom",z,"blah"
216.61.216.0/23,"thx0 Internet Services","thx0 Internet Services",7018,"thx0 Services, Inc."
216.61.218.0/24,"thx0 Internet Services","thx1",16788,"thx1, Inc."
1.0.4.0/22,m,m,56203,m
1.179.168.220/30,Itchy Feet,Itchy Feet,131293,"Itchy Feet Inc."
63.246.159.0/24,"United Airlines","United Airlines",395374,"BPA"
63.246.160.0/19,"Netflix","Netflix",11427,"Netflix, LLC"
"""


if __name__ == '__main__':

    if sys.version_info < (3,0):
        raise Exception("* needs python3 *")

    unittest.main()
