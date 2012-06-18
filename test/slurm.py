import random
import unittest
from pwd import getpwuid

import sys
sys.path.append("..")
 
from bart import slurm

node_list_examples = [
            { 'from': "brother13", 
                'ref': ["brother13"] },
            { 'from': "brother[13-14]",
                'ref': ["brother13","brother14"] },
            { 'from': "brother[13-18]",
                'ref': ['brother13', 'brother14', 'brother15', 'brother16', 'brother17', 'brother18'] },
            { 'from': "brother[13-14,16,19]",
                'ref': ['brother13', 'brother14', 'brother16', 'brother19'] },
            { 'from': "compute-3-29",
                'ref': ['compute-3-29'] },
            { 'from': "compute-10-[11,13-14,16]",
                'ref': ['compute-10-11', 'compute-10-13', 'compute-10-14', 'compute-10-16'] },
            { 'from': "compute-10-[11,13-14,16],compute-11-29",
                'ref': ['compute-10-11', 'compute-10-13', 'compute-10-14', 'compute-10-16', 'compute-11-29'] },
            { 'from': "compute-1-[0-1,3-18,20-24,26,28-30,32]",
                'ref': ['compute-1-0', 'compute-1-1', 'compute-1-3', 'compute-1-4', 'compute-1-5', 'compute-1-6', 'compute-1-7', 'compute-1-8', 'compute-1-9', 'compute-1-10', 'compute-1-11', 'compute-1-12', 'compute-1-13', 'compute-1-14', 'compute-1-15', 'compute-1-16', 'compute-1-17', 'compute-1-18', 'compute-1-20', 'compute-1-21', 'compute-1-22', 'compute-1-23', 'compute-1-24', 'compute-1-26', 'compute-1-28', 'compute-1-29', 'compute-1-30', 'compute-1-32'] },
            { 'from': "compute-11-12,compute-13-[25-26,28-32]",
                'ref': ['compute-11-12,compute-13-25', 'compute-11-12,compute-13-26', 'compute-11-12,compute-13-28', 'compute-11-12,compute-13-29', 'compute-11-12,compute-13-30', 'compute-11-12,compute-13-31', 'compute-11-12,compute-13-32'] },
            { 'from': "compute-14-[1-12,15,30-31]",
                'ref': ['compute-14-1', 'compute-14-2', 'compute-14-3', 'compute-14-4', 'compute-14-5', 'compute-14-6', 'compute-14-7', 'compute-14-8', 'compute-14-9', 'compute-14-10', 'compute-14-11', 'compute-14-12', 'compute-14-15', 'compute-14-30', 'compute-14-31'] },
            { 'from': "compute-2-[1-2,6-18,21,23,26-29]",
                'ref': ['compute-2-1', 'compute-2-2', 'compute-2-6', 'compute-2-7', 'compute-2-8', 'compute-2-9', 'compute-2-10', 'compute-2-11', 'compute-2-12', 'compute-2-13', 'compute-2-14', 'compute-2-15', 'compute-2-16', 'compute-2-17', 'compute-2-18', 'compute-2-21', 'compute-2-23', 'compute-2-26', 'compute-2-27', 'compute-2-28', 'compute-2-29'] },
            { 'from': "compute-4-[4-5,7-9,12-13,15-18,20-21,23-28,30-34]",
                'ref': ['compute-4-4', 'compute-4-5', 'compute-4-7', 'compute-4-8', 'compute-4-9', 'compute-4-12', 'compute-4-13', 'compute-4-15', 'compute-4-16', 'compute-4-17', 'compute-4-18', 'compute-4-20', 'compute-4-21', 'compute-4-23', 'compute-4-24', 'compute-4-25', 'compute-4-26', 'compute-4-27', 'compute-4-28', 'compute-4-30', 'compute-4-31', 'compute-4-32', 'compute-4-33', 'compute-4-34'] },
            { 'from': "compute-5-[2,5,9-11,13,15-16,22,26,28],compute-6-[28,31-34],compute-7-[2,4-5,7]",
                'ref': ['compute-5-2', 'compute-5-5', 'compute-5-9', 'compute-5-10', 'compute-5-11', 'compute-5-13', 'compute-5-15', 'compute-5-16', 'compute-5-22', 'compute-5-26', 'compute-5-28', 'compute-6-28', 'compute-6-31', 'compute-6-32', 'compute-6-33', 'compute-6-34', 'compute-7-2', 'compute-7-4', 'compute-7-5', 'compute-7-7'] },
            { 'from': "compute-10-[11,13-14,16],compute-1-[0-1,3-18,20-24,26,28-30,32],compute-11-12,compute-13-[25-26,28-32],compute-14-[1-12,15,30-31],compute-2-[1-2,6-18,21,23,26-29],compute-4-[4-5,7-9,12-13,15-18,20-21,23-28,30-34],compute-5-[2,5,9-11,13,15-16,22,26,28],compute-6-[28,31-34],compute-7-[2,4-5,7]", 
                'ref': ['compute-10-11', 'compute-10-13', 'compute-10-14', 'compute-10-16', 'compute-1-0', 'compute-1-1', 'compute-1-3', 'compute-1-4', 'compute-1-5', 'compute-1-6', 'compute-1-7', 'compute-1-8', 'compute-1-9', 'compute-1-10', 'compute-1-11', 'compute-1-12', 'compute-1-13', 'compute-1-14', 'compute-1-15', 'compute-1-16', 'compute-1-17', 'compute-1-18', 'compute-1-20', 'compute-1-21', 'compute-1-22', 'compute-1-23', 'compute-1-24', 'compute-1-26', 'compute-1-28', 'compute-1-29', 'compute-1-30', 'compute-1-32', 'compute-11-12,compute-13-25', 'compute-11-12,compute-13-26', 'compute-11-12,compute-13-28', 'compute-11-12,compute-13-29', 'compute-11-12,compute-13-30', 'compute-11-12,compute-13-31', 'compute-11-12,compute-13-32', 'compute-14-1', 'compute-14-2', 'compute-14-3', 'compute-14-4', 'compute-14-5', 'compute-14-6', 'compute-14-7', 'compute-14-8', 'compute-14-9', 'compute-14-10', 'compute-14-11', 'compute-14-12', 'compute-14-15', 'compute-14-30', 'compute-14-31', 'compute-2-1', 'compute-2-2', 'compute-2-6', 'compute-2-7', 'compute-2-8', 'compute-2-9', 'compute-2-10', 'compute-2-11', 'compute-2-12', 'compute-2-13', 'compute-2-14', 'compute-2-15', 'compute-2-16', 'compute-2-17', 'compute-2-18', 'compute-2-21', 'compute-2-23', 'compute-2-26', 'compute-2-27', 'compute-2-28', 'compute-2-29', 'compute-4-4', 'compute-4-5', 'compute-4-7', 'compute-4-8', 'compute-4-9', 'compute-4-12', 'compute-4-13', 'compute-4-15', 'compute-4-16', 'compute-4-17', 'compute-4-18', 'compute-4-20', 'compute-4-21', 'compute-4-23', 'compute-4-24', 'compute-4-25', 'compute-4-26', 'compute-4-27', 'compute-4-28', 'compute-4-30', 'compute-4-31', 'compute-4-32', 'compute-4-33', 'compute-4-34', 'compute-5-2', 'compute-5-5', 'compute-5-9', 'compute-5-10', 'compute-5-11', 'compute-5-13', 'compute-5-15', 'compute-5-16', 'compute-5-22', 'compute-5-26', 'compute-5-28', 'compute-6-28', 'compute-6-31', 'compute-6-32', 'compute-6-33', 'compute-6-34', 'compute-7-2', 'compute-7-4', 'compute-7-5', 'compute-7-7'] },
        ]

class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        pass

    def test_getNodes(self):
        for n in node_list_examples:
            print slurm.getNodes(n['from'])
            print n['ref']
            #print set(slurm.getNodes(n['from'])) ^ set(n['ref'])
            #print
            self.assertSetEqual(set(slurm.getNodes(n['from'])), set(n['ref']), "Set is not Equal")

    def test_getSeconds(self):
        print "Testing getSeconds()"
        self.assertEqual(slurm.getSeconds("1:0"), 1*60+0, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("1:1:0"), 1*3600+1*60+0, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("1-1:1:0"), 1*86400+1*3600+1*60+0, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("2:3"), 2*60+3, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("1:2:3"), 1*3600+2*60+3, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("1-2:3:4"), 1*86400+2*3600+3*60+4, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("2:3.1"), 2*60+3, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("1:2:3.2"), 1*3600+2*60+3, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("4-3:2:1.33"), 4*86400+3*3600+2*60+1, "Wrong number of seconds..")
        self.assertEqual(slurm.getSeconds("Smurf"), -1, "Failed to detect error...")
        self.assertEqual(slurm.getSeconds("1:a:3"), -1, "Failed to detect error...")
        self.assertEqual(slurm.getSeconds(""), -1, "Failed to detect error...")
        try:
            self.assertEqual(slurm.getSeconds("-1:2:3"), -1, "Failed to detect error...")
        except:
            pass
        
    def test_datetimeFromIsoStr(self):
        print "Testing datetimeFromIsoStr"
        self.assertEqual(slurm.datetimeFromIsoStr("2012-01-02T08:09:10").__str__(), "2012-01-02 08:09:10", "Not Equal..")
        self.assertEqual(slurm.datetimeFromIsoStr("2010-10-12T18:19:20").__str__(), "2010-10-12 18:19:20", "Not Equal..")
        
    def test_createUsageRecord(self):
        print "Testing createUsageRecord()"        
        log_entry = "90560|1000|batch|2012-06-12T17:37:43|2012-06-13T00:41:03|2012-06-18T00:41:29|snic020-11-15|5-00:00:26|00:00:00|96|t-cn[1014,1016]".split("|");
        # log_entry, hostname, user_map, project_map, missing_user_mappings, idtimestamp)
        ur = slurm.createUsageRecord(log_entry,'hostname.example.com',{},{},{},False)
        
        self.assertEqual(ur.record_id, "hostname.example.com:90560", "bad record_id")
        self.assertEqual(ur.local_job_id, "90560", "bad local_job_id")
        self.assertEqual(ur.global_job_id, "hostname.example.com:90560", "bad global_job_id")
        self.assertEqual(ur.local_user_id, getpwuid(1000)[0], "bad local_user_id")
        self.assertEqual(ur.global_user_name, None, "bad global_user_name")
        self.assertEqual(ur.machine_name, "hostname.example.com", "bad machine_name")
        self.assertEqual(ur.queue, "batch", "bad queue")
        self.assertEqual(ur.processors, "96", "bad processors")
        self.assertEqual(ur.node_count, 2, "bad node_count")
        self.assertEqual(ur.host, "t-cn1014,t-cn1016", "bad host")
        self.assertEqual(ur.submit_time, "2012-06-12T15:37:43Z", "bad submit_time")
        self.assertEqual(ur.start_time, "2012-06-12T22:41:03Z", "bad start_time %s" % ur.start_time)
        self.assertEqual(ur.end_time, "2012-06-17T22:41:29Z", "bad end_time %s" % ur.end_time)
        self.assertEqual(ur.cpu_duration, 0, "bad cpu_duration %s" % ur.cpu_duration)
        self.assertEqual(ur.wall_duration, 432026, "bad wall_duration %s" % ur.wall_duration)
        self.assertEqual(ur.project_name, "snic020-11-15", "bad project_name %s" % ur.project_name)
        self.assertEqual(ur.vo_info, [], "bad vo_info %s" % ur.vo_info)
        
        ur = slurm.createUsageRecord(log_entry,'hostname.example.com',{},{},{},True)
        
        self.assertEqual(ur.record_id, "hostname.example.com:90560:20120612224103", "bad record_id %s" % ur.record_id)
        self.assertEqual(ur.local_job_id, "90560", "bad local_job_id")
        self.assertEqual(ur.global_job_id, "hostname.example.com:90560", "bad global_job_id")
        self.assertEqual(ur.local_user_id, getpwuid(1000)[0], "bad local_user_id")
        self.assertEqual(ur.global_user_name, None, "bad global_user_name")
        self.assertEqual(ur.machine_name, "hostname.example.com", "bad machine_name")
        self.assertEqual(ur.queue, "batch", "bad queue")
        self.assertEqual(ur.processors, "96", "bad processors")
        self.assertEqual(ur.node_count, 2, "bad node_count")
        self.assertEqual(ur.host, "t-cn1014,t-cn1016", "bad host")
        self.assertEqual(ur.submit_time, "2012-06-12T15:37:43Z", "bad submit_time")
        self.assertEqual(ur.start_time, "2012-06-12T22:41:03Z", "bad start_time %s" % ur.start_time)
        self.assertEqual(ur.end_time, "2012-06-17T22:41:29Z", "bad end_time %s" % ur.end_time)
        self.assertEqual(ur.cpu_duration, 0, "bad cpu_duration %s" % ur.cpu_duration)
        self.assertEqual(ur.wall_duration, 432026, "bad wall_duration %s" % ur.wall_duration)
        self.assertEqual(ur.project_name, "snic020-11-15", "bad project_name %s" % ur.project_name)
        self.assertEqual(ur.vo_info, [], "bad vo_info %s" % ur.vo_info)
        
        missing_user_map = {}
        ur = slurm.createUsageRecord(log_entry,'hostname.example.com',{},{},missing_user_map,True)
        
        self.assertEqual(missing_user_map, {getpwuid(1000)[0]: True}, "bad missing_user_map %s" % missing_user_map)
        self.assertEqual(ur.record_id, "hostname.example.com:90560:20120612224103", "bad record_id %s" % ur.record_id)
        self.assertEqual(ur.local_job_id, "90560", "bad local_job_id")
        self.assertEqual(ur.global_job_id, "hostname.example.com:90560", "bad global_job_id")
        self.assertEqual(ur.local_user_id, getpwuid(1000)[0], "bad local_user_id")
        self.assertEqual(ur.global_user_name, None, "bad global_user_name")
        self.assertEqual(ur.machine_name, "hostname.example.com", "bad machine_name")
        self.assertEqual(ur.queue, "batch", "bad queue")
        self.assertEqual(ur.processors, "96", "bad processors")
        self.assertEqual(ur.node_count, 2, "bad node_count")
        self.assertEqual(ur.host, "t-cn1014,t-cn1016", "bad host")
        self.assertEqual(ur.submit_time, "2012-06-12T15:37:43Z", "bad submit_time")
        self.assertEqual(ur.start_time, "2012-06-12T22:41:03Z", "bad start_time %s" % ur.start_time)
        self.assertEqual(ur.end_time, "2012-06-17T22:41:29Z", "bad end_time %s" % ur.end_time)
        self.assertEqual(ur.cpu_duration, 0, "bad cpu_duration %s" % ur.cpu_duration)
        self.assertEqual(ur.wall_duration, 432026, "bad wall_duration %s" % ur.wall_duration)
        self.assertEqual(ur.project_name, "snic020-11-15", "bad project_name %s" % ur.project_name)
        self.assertEqual(ur.vo_info, [], "bad vo_info %s" % ur.vo_info)
        
        missing_user_map = {}
        ur = slurm.createUsageRecord(log_entry,'hostname.example.com',{ getpwuid(1000)[0] :"magnus@hpc2n.umu.se"},{},missing_user_map,True)
        
        self.assertEqual(missing_user_map, {}, "bad missing_user_map %s" % missing_user_map)
        self.assertEqual(ur.record_id, "hostname.example.com:90560:20120612224103", "bad record_id %s" % ur.record_id)
        self.assertEqual(ur.local_job_id, "90560", "bad local_job_id")
        self.assertEqual(ur.global_job_id, "hostname.example.com:90560", "bad global_job_id")
        self.assertEqual(ur.local_user_id, "magnus", "bad local_user_id")
        self.assertEqual(ur.global_user_name, "magnus@hpc2n.umu.se", "bad global_user_name %s" % ur.global_user_name)
        self.assertEqual(ur.machine_name, "hostname.example.com", "bad machine_name")
        self.assertEqual(ur.queue, "batch", "bad queue")
        self.assertEqual(ur.processors, "96", "bad processors")
        self.assertEqual(ur.node_count, 2, "bad node_count")
        self.assertEqual(ur.host, "t-cn1014,t-cn1016", "bad host")
        self.assertEqual(ur.submit_time, "2012-06-12T15:37:43Z", "bad submit_time")
        self.assertEqual(ur.start_time, "2012-06-12T22:41:03Z", "bad start_time %s" % ur.start_time)
        self.assertEqual(ur.end_time, "2012-06-17T22:41:29Z", "bad end_time %s" % ur.end_time)
        self.assertEqual(ur.cpu_duration, 0, "bad cpu_duration %s" % ur.cpu_duration)
        self.assertEqual(ur.wall_duration, 432026, "bad wall_duration %s" % ur.wall_duration)
        self.assertEqual(ur.project_name, "snic020-11-15", "bad project_name %s" % ur.project_name)
        self.assertEqual(ur.vo_info, [], "bad vo_info %s" % ur.vo_info)
        
        missing_user_map = {}
        ur = slurm.createUsageRecord(log_entry,'hostname.example.com',{getpwuid(1000)[0]:"magnus@hpc2n.umu.se"},{"snic020-11-15":"foo"},missing_user_map,True)
        
        self.assertEqual(missing_user_map, {}, "bad missing_user_map %s" % missing_user_map)
        self.assertEqual(ur.record_id, "hostname.example.com:90560:20120612224103", "bad record_id %s" % ur.record_id)
        self.assertEqual(ur.local_job_id, "90560", "bad local_job_id")
        self.assertEqual(ur.global_job_id, "hostname.example.com:90560", "bad global_job_id")
        self.assertEqual(ur.local_user_id, getpwuid(1000)[0], "bad local_user_id")
        self.assertEqual(ur.global_user_name, "magnus@hpc2n.umu.se", "bad global_user_name %s" % ur.global_user_name)
        self.assertEqual(ur.machine_name, "hostname.example.com", "bad machine_name")
        self.assertEqual(ur.queue, "batch", "bad queue")
        self.assertEqual(ur.processors, "96", "bad processors")
        self.assertEqual(ur.node_count, 2, "bad node_count")
        self.assertEqual(ur.host, "t-cn1014,t-cn1016", "bad host")
        self.assertEqual(ur.submit_time, "2012-06-12T15:37:43Z", "bad submit_time")
        self.assertEqual(ur.start_time, "2012-06-12T22:41:03Z", "bad start_time %s" % ur.start_time)
        self.assertEqual(ur.end_time, "2012-06-17T22:41:29Z", "bad end_time %s" % ur.end_time)
        self.assertEqual(ur.cpu_duration, 0, "bad cpu_duration %s" % ur.cpu_duration)
        self.assertEqual(ur.wall_duration, 432026, "bad wall_duration %s" % ur.wall_duration)
        self.assertEqual(ur.project_name, "snic020-11-15", "bad project_name %s" % ur.project_name)
        self.assertEqual(ur.vo_info[0].type, "lrmsurgen-projectmap", "bad vo_info.type %s" % ur.vo_info[0].type)
        self.assertEqual(ur.vo_info[0].name, "foo", "bad vo_info.name %s" % ur.vo_info[0].name)
        
if __name__ == '__main__':
    unittest.main()