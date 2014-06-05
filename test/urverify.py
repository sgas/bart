# -*- coding: utf-8 -*-

import unittest

import sys
sys.path.append("..")

import pprint

try:
    from xml.etree import cElementTree as ET
except ImportError:
    # Python 2.4 compatability
    from elementtree import ElementTree as ET


from bart.usagerecord import verify

urdata1 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:deisa="http://rmis.deisa.org/acct" xmlns:logger="http://www.sgas.se/namespaces/2010/08/logger" xmlns:ur="http://schema.ogf.org/urf/2003/09/urf"><ur:RecordIdentity ur:createTime="2014-06-05T11:18:25Z" ur:recordId="abisko.hpc2n.umu.se:1961149:20140605111520" /><ur:JobIdentity><ur:GlobalJobId>abisko.hpc2n.umu.se:1961149</ur:GlobalJobId><ur:LocalJobId>1961149</ur:LocalJobId></ur:JobIdentity><ur:UserIdentity><ur:LocalUserId>cernop05</ur:LocalUserId></ur:UserIdentity><ur:MachineName>abisko.hpc2n.umu.se</ur:MachineName><ur:Queue>grid</ur:Queue><ur:Host>t-cn1105</ur:Host><ur:NodeCount>1</ur:NodeCount><ur:Processors>1</ur:Processors><ur:ProjectName>grid-cernops</ur:ProjectName><deisa:SubmitTime>2014-06-05T11:15:20Z</deisa:SubmitTime><ur:StartTime>2014-06-05T11:15:20Z</ur:StartTime><ur:EndTime>2014-06-05T11:16:01Z</ur:EndTime><ur:WallDuration>PT41.000000S</ur:WallDuration><ur:CpuDuration>PT0.000000S</ur:CpuDuration><logger:LoggerName logger:version="trunk">SGAS-BaRT</logger:LoggerName></ur:JobUsageRecord>
"""

urdata2 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:deisa="http://rmis.deisa.org/acct" xmlns:logger="http://www.sgas.se/namespaces/2010/08/logger" xmlns:ur="http://schema.ogf.org/urf/2003/09/urf"><ur:RecordIdentity ur:createTime="2014-06-05T11:18:25Z" ur:recordId="abisko.hpc2n.umu.se:1961149:20140605111520" /><ur:JobIdentity><ur:GlobalJobId>abisko.hpc2n.umu.se:1961149</ur:GlobalJobId><ur:LocalJobId>1961149</ur:LocalJobId></ur:JobIdentity><ur:UserIdentity><ur:LocalUserId>cernop05</ur:LocalUserId></ur:UserIdentity><ur:MachineName>abisko.hpc2n.umu.se</ur:MachineName><ur:Queue>grid</ur:Queue><ur:Host>t-cn1105</ur:Host><ur:NodeCount>1</ur:NodeCount><ur:Processors>1</ur:Processors><ur:ProjectName>grid-cernops</ur:ProjectName><deisa:SubmitTime>2014-06-05T11:15:20Z</deisa:SubmitTime><ur:StartTime>2014-06-05T11:15:20Z</ur:StartTime><ur:EndTime>2014-06-05T11:16:01Z</ur:EndTime><ur:WallDuration>PT41.000000S</ur:WallDuration><ur:CpuDuration>PT0.000000S</ur:CpuDuration><logger:LoggerName logger:version="trunk">SGAS-BaRT</logger:LoggerName></ur:JobUsageRecordX>
"""

urdata3 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:deisa="http://rmis.deisa.org/acct" xmlns:logger="http://www.sgas.se/namespaces/2010/08/logger" xmlns:ur="http://schema.ogf.org/urf/2003/09/urf"><ur:RecordIdentity ur:createTime="2014-06-05T11:18:25Z" ur:recordId="abisko.hpc2n.umu.se:1961149:20140605111520" /><ur:JobIdentity><ur:GlobalJobId>abisko.hpc2n.umu.se:1961149</ur:GlobalJobId><ur:LocalJobId>1961149</ur:LocalJobId></ur:JobIdentity><ur:UserIdentity><ur:LocalUserId>cernop05</ur:LocalUserId></ur:UserIdentity><ur:MachineName>abisko.hpc2n.umu.se</ur:MachineName><ur:Queue>grid</ur:Queue><ur:Host>t-cn1105</ur:Host><ur:NodeCount>1</ur:NodeCount><ur:Processors>1</ur:Processors><ur:ProjectName>grid-cernops</ur:ProjectName><deisa:SubmitTime>2014-06-05T11:15:20Z</deisa:SubmitTime><ur:StartTime>2014-06-05T11:15:20Z</ur:StartTime><ur:EndTime>2014-06-05T11:16:01Z</ur:EndTime><ur:WallDuration>PT41.000000S</ur:WallDuration><ur:CpuDuration>PT0.000000S</ur:CpuDuration><logger:LoggerName logger:version="trunk">SGAS-BaRT</logger:LoggerName></ur:JobUsageRecord
"""

urdata4 = """<?xml version="1.0" encoding="UTF-8" ?>
"""

urdata5 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:deisa="http://rmis.deisa.org/acct" xmlns:logger="http://www.sgas.se/namespaces/2010/08/logger" xmlns:ur="http://schema.ogf.org/urf/2003/09/urf"><ur:JobIdentity><ur:GlobalJobId>abisko.hpc2n.umu.se:1961149</ur:GlobalJobId><ur:LocalJobId>1961149</ur:LocalJobId></ur:JobIdentity><ur:UserIdentity><ur:LocalUserId>cernop05</ur:LocalUserId></ur:UserIdentity><ur:MachineName>abisko.hpc2n.umu.se</ur:MachineName><ur:Queue>grid</ur:Queue><ur:Host>t-cn1105</ur:Host><ur:NodeCount>1</ur:NodeCount><ur:Processors>1</ur:Processors><ur:ProjectName>grid-cernops</ur:ProjectName><deisa:SubmitTime>2014-06-05T11:15:20Z</deisa:SubmitTime><ur:StartTime>2014-06-05T11:15:20Z</ur:StartTime><ur:EndTime>2014-06-05T11:16:01Z</ur:EndTime><ur:WallDuration>PT41.000000S</ur:WallDuration><ur:CpuDuration>PT0.000000S</ur:CpuDuration><logger:LoggerName logger:version="trunk">SGAS-BaRT</logger:LoggerName></ur:JobUsageRecord>
"""

class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        pass
    
    def verify(self,urdata):
        try:
            ur = ET.fromstring(urdata)
        except:
            return False
        return verify.verify(ur)

    def test_parser(self):
        self.assertTrue(self.verify(urdata1))
        self.assertFalse(self.verify(urdata2))
        self.assertFalse(self.verify(urdata3))
        self.assertFalse(self.verify(urdata4))
        self.assertFalse(self.verify(urdata5))
        pass
        
if __name__ == '__main__':
    unittest.main()
