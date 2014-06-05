#
# Maui log parsing and UR generation
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Author: Magnus Jonsson <magnus@hpc2n.umu.se>
# Copyright: Nordic Data Grid Facility (2009, 2010)

import os
import time
import logging

from bart import common
from bart.usagerecord import usagerecord

MAUI_DATE_FORMAT = '%a_%b_%d_%Y'
STATS_DIR        = 'stats'
MAUI_CFG_FILE    = 'maui.cfg'

SECTION = 'maui'

STATEFILE = 'statefile'
DEFAULT_STATEFILE = SECTION + '.state'

SPOOL_DIR = 'spooldir'
DEFAULT_SPOOL_DIR = '/var/spool/maui'

CONFIG = {
            STATEFILE:       { 'required': False },
            SPOOL_DIR:       { 'required': False },
          }

class MauiLogParser:
    """
    Parser for maui stats log.
    """
    def __init__(self, log_file):
        self.log_file = log_file
        self.file_ = None


    def openFile(self):
        self.file_ = open(self.log_file)


    def splitLineEntry(self, line):
        fields = [ e.strip() for e in line.split(' ') if e != '' ]
        return fields


    def getNextLogLine(self):
        if self.file_ is None:
            self.openFile()

        while True:
            line = self.file_.readline()
            if line.startswith('VERSION'):
                continue # maui log files starts with a version, typically 230
            if line.startswith('#'):
                continue # maui somtimes creates explanatory lines in the log file
            if line == '\n':
                continue # Ignore empty lines
            if line == '': # last line
                return None

            return line


    def getNextLogEntry(self):
        line = self.getNextLogLine()
        if line is None:
            return None
        return self.splitLineEntry(line)


    def spoolToEntry(self, entry_id):
        while True:
            log_entry = self.getNextLogEntry()
            if log_entry is None or log_entry[0] == entry_id:
                break

class Maui():

    state_job_id = None
    state_log_file = None    
    missing_user_mappings = {}
        
    def __init__(self,cfg):
        self.cfg = cfg
        
    def getStateFile(self):
        """
        Return the name of the statefile
        """
        return self.cfg.getConfigValue(SECTION, STATEFILE, DEFAULT_STATEFILE)
       
    def getMauiServer(self, maui_spool_dir):
    
        SERVERHOST = 'SERVERHOST'
    
        maui_cfg_path = os.path.join(maui_spool_dir, MAUI_CFG_FILE)
    
        if os.path.exists(maui_cfg_path):
            for line in file(maui_cfg_path):
                line = line.strip()
                if line.startswith(SERVERHOST):
                    entry = line.replace(SERVERHOST,'').strip()
                    return entry
    
        logging.warning('Could not get Maui server host setting')
        return None
    
    
    
    def createUsageRecord(self, log_entry, hostname, user_map, vo_map, maui_server_host):
        """
        Creates a Usage Record object given a Maui log entry.
        """
    
        # extract data from the workload trace (log_entry)
    
        job_id       = log_entry[0]
        user_name    = log_entry[3]
        req_class    = log_entry[7]
        submit_time  = int(log_entry[8])
        start_time   = int(log_entry[10])
        end_time     = int(log_entry[11])
        alo_tasks    = int(log_entry[21])
        account_name = log_entry[25]
        utilized_cpu = float(log_entry[29])
        core_count   = int(log_entry[31])*alo_tasks
        hosts        = log_entry[37].split(':')
    
        # clean data and create various composite entries from the work load trace
    
        if job_id.isdigit() and maui_server_host is not None:
            job_identifier = job_id + '.' + maui_server_host
        else:
            job_identifier = job_id
        fqdn_job_id = hostname + ':' + job_identifier
    
        if not user_name in user_map:
            self.missing_user_mappings[user_name] = True
    
        queue = req_class.replace('[','').replace(']','')
        if ':' in queue:
            queue = queue.split(':')[0]
    
        if account_name == '[NONE]':
            account_name = None
    
        mapped_vo = None
        if account_name is not None:
            mapped_vo = vo_map.get(account_name)
        if mapped_vo is None:
            mapped_vo = vo_map.get(user_name)
    
        vo_info = []
        if mapped_vo is not None:
            voi = usagerecord.VOInformation(name=mapped_vo, type_='bart-vomap')
            vo_info = [voi]
    
        wall_time = end_time - start_time
    
        # okay, this is somewhat ridiculous and complicated:
        # When compiled on linux, maui will think that it will only get cputime reading
        # from the master node. To compensate for this it multiples the utilized cpu field
        # with the number of tasks. However on most newer torque installations the correct
        # cpu utilization is reported. When combined this creates abnormally high cpu time
        # values for parallel jobs. The following heuristic tries to compensate for this,
        # by checking if the cpu time is higher than wall_time * cpus (which it never should
        # be), and then correct the number. However this will not work for jobs with very
        # low efficiancy
    
        if utilized_cpu > wall_time * alo_tasks:
            utilized_cpu /= alo_tasks
    
        ## fill in usage record fields
    
        ur = usagerecord.UsageRecord()
    
        ur.record_id = fqdn_job_id
    
        ur.local_job_id = job_identifier
        ur.global_job_id = fqdn_job_id
    
        ur.local_user_id = user_name
        ur.global_user_name = user_map.get(user_name)
    
        ur.machine_name = hostname
        ur.queue = queue
    
        ur.processors = core_count
        ur.node_count = len(hosts)
        ur.host = ','.join(hosts)
    
        ur.submit_time = usagerecord.epoch2isoTime(submit_time)
        ur.start_time  = usagerecord.epoch2isoTime(start_time)
        ur.end_time    = usagerecord.epoch2isoTime(end_time)
    
        ur.cpu_duration = utilized_cpu
        ur.wall_duration = wall_time
    
        ur.project_name = account_name
        ur.vo_info = vo_info
    
        return ur
    
    
    
    def shouldGenerateUR(self, log_entry, user_map):
        """
        Decides wheater a log entry is 'suitable' for generating
        a ur from.
        """
        job_id    = log_entry[0]
        user_name = log_entry[3]
        job_state = log_entry[6]
    
        if not job_state == 'Completed':
            logging.info('Job %s: Skipping UR generation (state %s)' % (job_id, job_state))
            return False
        if user_name in user_map and user_map[user_name] is None:
            logging.info('Job %s: User configured to skip UR generation' % job_id)
            return False
    
        return True
    
    
    
    def generateUsageRecords(self, hostname, user_map, vo_map):
        """
        Starts the UR generation process.
        """
    
        maui_spool_dir = self.cfg.getConfigValue(SECTION, SPOOL_DIR, DEFAULT_SPOOL_DIR)
        maui_server_host = self.getMauiServer(maui_spool_dir)
        maui_date_today = time.strftime(MAUI_DATE_FORMAT, time.gmtime())

        # set initial job_id        
        job_id    = self.state_job_id
        maui_date = self.state_log_file
    
        self.missing_user_mappings = {}
    
        while True:
    
            log_file = os.path.join(maui_spool_dir, STATS_DIR, maui_date)
            mlp = MauiLogParser(log_file)
            if job_id is not None:
                mlp.spoolToEntry(job_id)
    
            while True:
    
                try:
                    log_entry = mlp.getNextLogEntry()
                except IOError:
                    if maui_date == maui_date_today: # todays entry might not exist yet
                        break
                    logging.error('Error opening log file at %s for date %s' % (log_file, maui_date))
                    break
    
                if log_entry is None:
                    break # no more log entries
    
                if len(log_entry) != 44:
                    logging.error('Read entry with an invalid number fields:')
                    logging.error(' - File %s contains entry with %i fields. First field: %s' % (log_file, len(log_entry), log_entry[0]))
                    logging.error(' - No usage record will be generated from this line')
                    continue
    
                job_id = log_entry[0]
                if not self.shouldGenerateUR(log_entry, user_map):
                    logging.debug('Job %s: No UR will be generated.' % job_id)
                    continue
    
                ur = self.createUsageRecord(log_entry, hostname, user_map, vo_map, maui_server_host)
                common.writeUr(ur,cfg)
                
                # write generated state
                self.state_job_id = ur.job_id
                self.state_log_file = maui_date
                common.writeGeneratorState(self)
    
                job_id = None
    
            if maui_date == maui_date_today:
                break
    
            maui_date = common.getIncrementalDate(maui_date, MAUI_DATE_FORMAT)
            job_id = None
    
    def parseGeneratorState(self,state):        
        """
        Get state of where to the UR generation has reached in the log.
        """
        job_id = None
        log_file = None
        if state is None or len(state) == 0:            
            # Empty state data happens sometimes, usually NFS is involved :-|
            # Start from yesterday (24 hours back), this should be fine assuming (at least) daily invokation.
            t_old = time.time() - (24 * 3600)
            log_file = time.strftime(MAUI_DATE_FORMAT, time.gmtime(t_old))
        else:
            job_id, log_file = state_data.split(' ', 2)
            if job_id == '-':
                job_id = None

        self.state_job_id = job_id
        self.state_log_file = log_file

    def createGeneratorState(self):
        """
        Create the current state of where to the UR generation has reached.
        """
        return '%s %s' % (self.state_job_id or '-', self.state_log_file)