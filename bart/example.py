#
# Example accounting parsing -> UR module
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Magnus Jonsson <magnus@hpc2n.umu.se>
# Copyright: Nordic Data Grid Facility (2010)

import time
import datetime
import logging
import re

from bart import usagerecord, common

SECTION = 'example'

STATEFILE = 'statefile'
DEFAULT_STATEFILE = SECTION + '.state'

STATEFILE_START = 'statefile_start'
DEFAULT_STATEFILE_START = 1

IDTIMESTAMP = 'idtimestamp'
DEFAULT_IDTIMESTAMP = 'false'

CONFIG = {
            STATEFILE:       { 'required': False },
            STATEFILE_START: { 'required': False, type: 'int' },
            IDTIMESTAMP:     { 'required': False, type: 'bool' },
          }


class Example:   
    
    state = None
    cfg = None
    missing_user_mappings = None
    idtimestamp = DEFAULT_IDTIMESTAMP
    
    def __init__(self,cfg):
        self.cfg = cfg
        self.idtimestamp = cfg.getConfigValueBool(SECTION, IDTIMESTAMP, DEFAULT_IDTIMESTAMP)
        
    def getStateFile(self):
        """
        Return the name of the statefile
        """
        return self.cfg.getConfigValue(SECTION, STATEFILE, DEFAULT_STATEFILE)
    
    def generateUsageRecords(self, hostname, user_map, project_map):
        """
        Starts the UR generation process.
        """
        self.missing_user_mappings = {}
        
        # Creates 5 Usage Record object
        for count in [1,2,3,4,5]:
            self.state = self.state + 1
            
            # create some data at random...
            job_id       = str(self.state)      
            account_name = 'default'
            user_name    = 'default'
            submit_time  = time.mktime(common.datetimeFromIsoStr('2012-01-01T00:00:00').timetuple())
            start_time   = time.mktime(common.datetimeFromIsoStr('2012-01-02T01:23:45').timetuple())
            end_time     = time.mktime(common.datetimeFromIsoStr('2012-01-02T02:34:56').timetuple())

            # clean data and create various composite entries from the work load trace
            fqdn_job_id = hostname + ':' + job_id
            if self.idtimestamp:
                record_id_timestamp = re.sub("[-:TZ]","",usagerecord.epoch2isoTime(start_time)) # remove characters
                record_id = fqdn_job_id + ':' + record_id_timestamp
            else:
                record_id = fqdn_job_id

            if not user_name in user_map.getMapping():
                self.missing_user_mappings[user_name] = True

            vo_info = []
            if account_name is not None:
                mapped_project = project_map.get(account_name)
                if mapped_project is not None:
                    voi = usagerecord.VOInformation()
                    voi.type = 'lrmsurgen-projectmap'
                    voi.name = mapped_project
                    vo_info = [voi]

            ## fill in usage record fields
            ur = usagerecord.UsageRecord()
            ur.record_id        = record_id
            ur.local_job_id     = job_id
            ur.global_job_id    = fqdn_job_id
            ur.local_user_id    = user_name
            ur.global_user_name = user_map.get(user_name)
            ur.machine_name     = hostname
            ur.queue            = 'default'
            ur.processors       = 1
            ur.node_count       = 1
            ur.host             = hostname
            ur.submit_time      = usagerecord.epoch2isoTime(submit_time)
            ur.start_time       = usagerecord.epoch2isoTime(start_time)
            ur.end_time         = usagerecord.epoch2isoTime(end_time)
            ur.cpu_duration     = 90
            ur.wall_duration    = 100
            ur.project_name     = account_name
            ur.vo_info         += vo_info

            common.writeUr(ur,self.cfg)
        
    def parseGeneratorState(self,state):        
        """
        Get state of where to the UR generation has reached in the log.
        """
        if state is None or len(state) == 0:            
            state = int(self.cfg.getConfigValue(SECTION, STATEFILE_START, DEFAULT_STATEFILE_START))            

        logging.debug("State read is: %d" % int(state))
        self.state = int(state)

    def createGeneratorState(self):
        """
        Create the current state of where to the UR generation has reached.
        """
        return str(self.state)
