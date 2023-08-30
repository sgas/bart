#
# Torque log parser and UR generator
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Author: Andreas Engelbredt Dalsgaard <andreas.dalsgaard@gmail.com>
# Author: Magnus Jonsson <magnus@hpc2n.umu.se>
# Copyright: Nordic Data Grid Facility (2009, 2010)

import os
import time
import logging

from bart import common
from bart.usagerecord import usagerecord

SECTION = 'torque'

STATEFILE = 'statefile'
DEFAULT_STATEFILE = SECTION + '.state'

SPOOL_DIR = 'spooldir'
DEFAULT_SPOOL_DIR = '/var/spool/torque'

CONFIG = {
            STATEFILE:       { 'required': False },
            SPOOL_DIR:       { 'required': False },
          }

TORQUE_DATE_FORMAT = '%Y%m%d'

class TorqueLogParser:
    """
    Parser for torque accounting log.
    """
    def __init__(self, log_file):
        self.log_file = log_file
        self.file_ = None


    def openFile(self):
        self.file_ = open(self.log_file)


    def splitLineEntry(self, line):
        line_tokens = line.split(' ')
        fields = {}

        start_fields = line_tokens[1].split(';')
        fields['entrytype'] = start_fields[1]
        fields['jobid'] = start_fields[2]
        fields['user'] = start_fields[3].split('=')[1]

        for e in line_tokens:
            e = e.strip()
            r = e.split('=')
            if len(r) >= 2:
                fields[r[0]] = '='.join(r[1:len(r)])

        return fields


    def getNextLogLine(self):
        if self.file_ is None:
            self.openFile()

        while True:
            line = self.file_.readline()
            if line == '': #last line
                return None
            if line[20] == 'E':
                return line


    def getNextLogEntry(self):
        line = self.getNextLogLine()
        if line is None:
            return None
        return self.splitLineEntry(line)


    def spoolToEntry(self, entry_id):
        while True:
            log_entry = self.getNextLogEntry()
            if log_entry is None or log_entry['jobid'] == entry_id:
                break

class Torque:
    state = None
    cfg = None
    missing_user_mappings = None
    
    def __init__(self, cfg):
        self.cfg = cfg
        
    def getStateFile(self):
        """
        Return the name of the statefile
        """
        return self.cfg.getConfigValue(SECTION, STATEFILE, DEFAULT_STATEFILE)

    def getCoreCount(self,nodes):
        """
        Find number of cores used by parsing the Resource_List.nodes value
        {<node_count> | <hostname>} [:ppn=<ppn>][:<property>[:<property>]...] [+ ...]
        http://www.clusterresources.com/torquedocs21/2.1jobsubmission.shtml#nodeExamples
        """
        cores = 0
        for node_req in nodes.split('+'):
            listTmp = node_req.split(':')
            if listTmp[0].isdigit():
                first = int(listTmp[0])
            else:
                first = 1

            cores += first
            if len(listTmp) > 1:
                for e in listTmp:
                    if len(e) > 3:
                        if e[0:3] == 'ppn':
                            cores -= first
                            cores += first*int(e.split('=')[1])
                            break
        return cores
    
    def getSeconds(self,torque_timestamp):
        """
        Convert time string in the form HH:MM:SS to seconds
        """
        (hours, minutes, seconds) = torque_timestamp.split(':')
        return int(hours)*3600 + int(minutes)*60 + int(seconds)
    
    def createUsageRecord(self, log_entry, hostname, user_map, vo_map):
        """
        Creates a Usage Record object given a Torque log entry.
        """

        # extract data from the workload trace (log_entry)
        job_id       = log_entry['jobid']
        user_name    = log_entry['user']
        queue        = log_entry['queue']
        account      = log_entry.get('account')
        submit_time  = int(log_entry['ctime'])
        start_time   = int(log_entry['start'])
        end_time     = int(log_entry['end'])
        utilized_cpu = self.getSeconds(log_entry['resources_used.cput'])
        wall_time    = self.getSeconds(log_entry['resources_used.walltime'])

        hosts = list(set([hc.split('/')[0] for hc in log_entry['exec_host'].split('+')]))

        # initial value
        node_count = len(hosts)

        if log_entry.has_key('Resource_List.ncpus'):
            core_count = int(log_entry['Resource_List.ncpus'])
        elif log_entry.has_key('Resource_List.nodes'):
            core_count = self.getCoreCount(log_entry['Resource_List.nodes'])
        # mppwidth is used on e.g. Cray machines instead of ncpus / nodes
        elif log_entry.has_key('Resource_List.mppwidth') or log_entry.has_key('Resource_List.size'):
            if log_entry.has_key('Resource_List.mppwidth'):
                core_count = int(log_entry['Resource_List.mppwidth'])
            # older versions on e.g. Cray machines use "size" as keyword for mppwidth or core_count
            elif log_entry.has_key('Resource_List.size'):
                core_count = int(log_entry['Resource_List.size'])
            # get node count, mppnodect exist only in newer versions
            if log_entry.has_key('Resource_List.mppnodect'):
                node_count = int(log_entry['Resource_List.mppnodect'])
            else:
                logging.warning('Missing mppnodect for entry: %s (will guess from "core count"/mppnppn)' % job_id)
                try:
                    node_count = core_count / int(log_entry['Resource_List.mppnppn'])
                except:
                    logging.warning('Unable to calculate node count for entry: %s (will guess from host list)' % job_id)
                    # keep the default of len(hosts) given above
        else:
            logging.warning('Missing processor count for entry: %s (will guess from host list)' % job_id)
            # assume the number of exec hosts is the core count (possibly not right)
            core_count = len(hosts)

        # clean data and create various composite entries from the work load trace
        if job_id.isdigit() and hostname is not None:
            job_identifier = job_id + '.' + hostname
        else:
            job_identifier = job_id
        fqdn_job_id = hostname + ':' + job_identifier

        if not user_map.get(user_name):
            self.missing_user_mappings[user_name] = True

        vo_info = []
        if account:
            mapped_vo = vo_map.get(account)
        else:
            mapped_vo = vo_map.get(user_name)
        if mapped_vo is not None:
            voi = usagerecord.VOInformation(name=mapped_vo, type_='bart-vomap')
            vo_info.append(voi)

        ## fill in usage record fields
        ur = usagerecord.UsageRecord()
        ur.record_id        = fqdn_job_id
        ur.local_job_id     = job_identifier
        ur.global_job_id    = fqdn_job_id
        ur.local_user_id    = user_name
        ur.global_user_name = user_map.get(user_name)
        ur.machine_name     = hostname
        ur.queue            = queue
        ur.project_name     = account
        ur.processors       = core_count
        ur.node_count       = node_count
        ur.host             = ','.join(hosts)
        ur.submit_time      = usagerecord.epoch2isoTime(submit_time)
        ur.start_time       = usagerecord.epoch2isoTime(start_time)
        ur.end_time         = usagerecord.epoch2isoTime(end_time)
        ur.cpu_duration     = utilized_cpu
        ur.wall_duration    = wall_time
        ur.vo_info         += vo_info
        ur.exit_code        = log_entry['Exit_status']

        return ur


    def generateUsageRecords(self,hostname, user_map, vo_map):
        """
        Starts the UR generation process.
        """

        torque_spool_dir = self.cfg.getConfigValue(SECTION, SPOOL_DIR, DEFAULT_SPOOL_DIR)
        torque_accounting_dir = os.path.join(torque_spool_dir, 'server_priv', 'accounting')

        torque_date_today = time.strftime(TORQUE_DATE_FORMAT, time.gmtime())
        
        job_id = self.state_job_id
        torque_date = self.state_log_file

        self.missing_user_mappings = {}

        while True:
            log_file = os.path.join(torque_accounting_dir, torque_date)
            tlp = TorqueLogParser(log_file)
            if job_id is not None:
                try:
                    tlp.spoolToEntry(job_id)
                except IOError as e:
                    logging.error('Error spooling log file at %s for date %s to %s (%s)' % (log_file, torque_date, job_id, str(e)) )
                    job_id = None
                    continue

            while True:
                try:
                    log_entry = tlp.getNextLogEntry()
                except IOError as e:
                    if torque_date == torque_date_today: # todays entry might not exist yet
                        break
                    logging.error('Error reading log file at %s for date %s (%s)' % (log_file, torque_date, str(e)))
                    break

                if log_entry is None:
                    break # no more log entries

                job_id = log_entry['jobid']

                ur = self.createUsageRecord(log_entry, hostname, user_map, vo_map)
                common.writeUr(ur,self.cfg)
                
                self.state_job_id = job_id
                self.state_log_file = torque_date
                common.writeGeneratorState(self)
                
                job_id = None

            if torque_date == torque_date_today:
                break

            torque_date = common.getIncrementalDate(torque_date, TORQUE_DATE_FORMAT)
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
            log_file = time.strftime(TORQUE_DATE_FORMAT, time.gmtime(t_old))
        else:
            job_id, log_file = state.split(' ', 2)
            if job_id == '-':
                job_id = None

        self.state_job_id = job_id
        self.state_log_file = log_file

    def createGeneratorState(self):
        """
        Create the current state of where to the UR generation has reached.
        """
        return '%s %s' % (self.state_job_id or '-', self.state_log_file)
