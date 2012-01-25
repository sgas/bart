#
# Slurm accounting log parsing -> UR module
#
# Module for the  SGAS Batch system Reporting Tool (BaRT).
#
# Author: Andreas Engelbredt Dalsgaard <andreas.dalsgaard@gmail.com>
# Copyright: Nordic Data Grid Facility (2010)

import os
import time
import datetime
import logging
import subprocess
import sys

from common import getStateFileLocation
from bart import config, usagerecord
from pwd import getpwuid



STATE_FILE       = 'slurm.state'
COMMAND          = 'sacct --allusers --parsable2 --format=JobID,UID,Partition,Submit,Start,End,Account,Elapsed,UserCPU,AllocCPUS,Nodelist --allocations --state=ca,cd,f,nf,to --starttime="%s" --endtime="%s"'



class SlurmBackend:
    """
    DB backend for slurm accounting.
    """
    def __init__(self, state_starttime):

        self.end_str = datetime.datetime.now().isoformat().split('.')[0]
        command = COMMAND % (state_starttime, self.end_str)

        # subprocess can be more than 10x times slower than popen in python2.4
        if sys.version_info < (2, 5):
            process = os.popen(command)
            self.results = process.readlines()
        else:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            self.results = process.stdout.readlines()
            os.waitpid(process.pid, 0)

        # remove description line
        self.results = self.results[1:]


    def getNextLogEntry(self):

        try:
            entry = self.results.pop(0)
            return entry[:-1].split('|')
        except IndexError:
            return None



def getNodes(node_str):
    """
    Makes a list of nodes from strings like:

    "brother[13-14]"
    "brother13"
    "brother[13-14,16,19]"
    "brother[13-18]"
    "compute-3-29"
    "compute-10-[11,13-14,16]",
    "compute-1-[0-1,3-18,20-24,26,28-30,32],compute-11-12,compute-13-[25-26,28-32],compute-14-[1-12,15,30-31],compute-2-[1-2,6-18,21,23,26-29],compute-4-[4-5,7-9,12-13,15-18,20-21,23-28,30-34],compute-5-[2,5,9-11,13,15-16,22,26,28],compute-6-[28,31-34],compute-7-[2,4-5,7]"]
    """
    nodes = []
    if '],' in node_str:
        elements = node_str.split('],')
    else:
        elements = [node_str]

    for element in elements:
        if '[' in element:
            parts = element.split('[')

            for sequence in parts[1].split(','):
                sequence = sequence.rstrip(']')
                if '-' in  sequence:
                    numbers = sequence.split('-')

                    for i in range(int(numbers[0]),int(numbers[1])+1):
                        nodes.append(parts[0] + str(i))
                else:
                    nodes.append(parts[0] + sequence)
        else:
            nodes += [element]

    return nodes



def getSeconds(time_str):
    """
    Convert a string of the form '%d-%H:%M:%S', '%H:%M:%S' or '%M:%S'
    to seconds.
    """
    # sometimes the timestamp includs a fractional second part
    time_str = time_str.split('.')[0]

    if '-' in time_str:
        days, time_str = time_str.split('-')
        st = time.strptime(time_str, '%H:%M:%S')
        sec = int(days)*86400+st.tm_hour*3600+st.tm_min*60+st.tm_sec
    else:
        try:
            st = time.strptime(time_str, '%H:%M:%S')
            sec = st.tm_hour*3600+st.tm_min*60+st.tm_sec
        except ValueError:
            try:
                st = time.strptime(time_str, '%M:%S')
                sec = st.tm_min*60+st.tm_sec
            except ValueError:
                logging.info('String: %s does not match time format.' % time_str)
                return -1

    return sec



def datetimeFromIsoStr(dt_str):
    """
    Convert a iso time string to datetime. 
    """
    dt_split = dt_str.split(".")
    # why the parameter unfold?
    return datetime.datetime(*(time.strptime(dt_split[0], "%Y-%m-%dT%H:%M:%S")[0:6]))



def createUsageRecord(log_entry, hostname, user_map, project_map, missing_user_mappings, idtimestamp):
    """
    Creates a Usage Record object given a slurm log entry.
    """

    # extract data from the workload trace (log_entry)
    job_id       = str(log_entry[0])
    user_name    = getpwuid(int(log_entry[1]))[0]
    queue        = log_entry[2]
    submit_time  = time.mktime(datetimeFromIsoStr(log_entry[3]).timetuple())
    start_time   = time.mktime(datetimeFromIsoStr(log_entry[4]).timetuple())
    end_time     = time.mktime(datetimeFromIsoStr(log_entry[5]).timetuple())
    account_name = log_entry[6]
    utilized_cpu = getSeconds(log_entry[8])
    wall_time    = getSeconds(log_entry[7])
    core_count   = log_entry[9]
    hosts        = getNodes(log_entry[10])

    # clean data and create various composite entries from the work load trace
    job_identifier = job_id
    fqdn_job_id = hostname + ':' + job_id
    if idtimestamp:
        record_id = fqdn_job_id + ':' + start_time.replace('-','').replace(':','').upper()
    else:
        record_id = fqdn_job_id

    if not user_name in user_map:
        missing_user_mappings[user_name] = True

    vo_info = []
    if account_name is not None:
        mapped_project = project_map.get(account_name)
        if mapped_project is not None:
            voi = usagerecord.VOInformation()
            voi.type = 'lrmsurgen-projectmap'
            voi.name = mapped_project

    ## fill in usage record fields
    ur = usagerecord.UsageRecord()
    ur.record_id        = record_id
    ur.local_job_id     = job_identifier
    ur.global_job_id    = fqdn_job_id
    ur.local_user_id    = user_name
    ur.global_user_name = user_map.get(user_name)
    ur.machine_name     = hostname
    ur.queue            = queue
    ur.processors       = core_count
    ur.node_count       = len(hosts)
    ur.host             = ','.join(hosts)
    ur.submit_time      = usagerecord.epoch2isoTime(submit_time)
    ur.start_time       = usagerecord.epoch2isoTime(start_time)
    ur.end_time         = usagerecord.epoch2isoTime(end_time)
    ur.cpu_duration     = utilized_cpu
    ur.wall_duration    = wall_time
    ur.project_name     = account_name
    ur.vo_info         += vo_info

    return ur



def getGeneratorState(cfg):
    """
    Get state of where to the UR generation has reached in the log.
    This is returns the last jobid processed from the database.
    """
    state_file = getStateFileLocation(cfg)
    if not os.path.exists(state_file):
        # no statefile -> we start from 50000 seconds / 5.7 days ago
        dt = datetime.datetime.now()-datetime.timedelta(seconds=50000)
        return dt.isoformat().split('.')[0]

    return open(state_file).readline().strip() # state is only on the first line



def writeGeneratorState(cfg, state_time):
    """
    Write the state of where the logs have been parsed to.
    This is a job id from the database.
    """
    state_file = getStateFileLocation(cfg)

    dirpath = os.path.dirname(state_file)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath, mode=0750)

    f = open(state_file, 'w')
    f.write(state_time)
    f.close()



def generateUsageRecords(cfg, hostname, user_map, project_map, idtimestamp):
    """
    Starts the UR generation process.
    """
    start_time = getGeneratorState(cfg)
    missing_user_mappings = {}

    tlp = SlurmBackend(start_time)
    end_time = tlp.end_str

    while True:

        log_entry = tlp.getNextLogEntry()

        if log_entry is None:
            break # no more log entries

        ur = createUsageRecord(log_entry, hostname, user_map, project_map, missing_user_mappings, idtimestamp)
        log_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.LOGDIR, config.DEFAULT_LOG_DIR)
        ur_dir = os.path.join(log_dir, 'urs')
        if not os.path.exists(ur_dir):
            os.makedirs(ur_dir)

        ur_file = os.path.join(ur_dir, ur.record_id)
        ur.writeXML(ur_file)

        logging.info('Wrote usage record to %s' % ur_file)

    writeGeneratorState(cfg, end_time)

    if missing_user_mappings:
        users = ','.join(missing_user_mappings)
        logging.info('Missing user mapping for the following users: %s' % users)




def test():

    node_list_examples = [
        "brother13",
        "brother[13-14]",
        "brother[13-18]",
        "brother[13-14,16,19]",
        "compute-3-29",
        "compute-10-[11,13-14,16]",
        "compute-10-[11,13-14,16],compute-11-29",
        "compute-1-[0-1,3-18,20-24,26,28-30,32]",
        "compute-11-12,compute-13-[25-26,28-32]",
        "compute-14-[1-12,15,30-31]",
        "compute-2-[1-2,6-18,21,23,26-29]",
        "compute-4-[4-5,7-9,12-13,15-18,20-21,23-28,30-34]",
        "compute-5-[2,5,9-11,13,15-16,22,26,28],compute-6-[28,31-34],compute-7-[2,4-5,7]",
        "compute-10-[11,13-14,16],compute-1-[0-1,3-18,20-24,26,28-30,32],compute-11-12,compute-13-[25-26,28-32],compute-14-[1-12,15,30-31],compute-2-[1-2,6-18,21,23,26-29],compute-4-[4-5,7-9,12-13,15-18,20-21,23-28,30-34],compute-5-[2,5,9-11,13,15-16,22,26,28],compute-6-[28,31-34],compute-7-[2,4-5,7]"
    ]

    for n in node_list_examples:
        print n
        print getNodes(n)
        print


def main():
    test()

if __name__ == '__main__':
    main()


