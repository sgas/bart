#
# Common functions for LRMS log parsing
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Author: Andreas Engelbredt Dalsgaard <andreas.dalsgaard@gmail.com>
# Author: Magnus Jonsson <magnus@hpc2n.umu.se>
# Copyright: Nordic Data Grid Facility (2009, 2010)


import os
import time
import datetime
import logging

from bart import config

def getIncrementalDate(date, date_format):
    """
    Returns the following day in date format given as argument, given a date
    in that date format.
    """
    gm_td = time.strptime(date, date_format)
    d = datetime.date(gm_td.tm_year, gm_td.tm_mon, gm_td.tm_mday)
    day = datetime.timedelta(days=1)
    d2 = d + day
    next_date = time.strftime(date_format, d2.timetuple())
    return next_date

def readGeneratorState(lrms):        
    """
    Get the content of state file provided by lrmsObj
    """
    state_dir = lrms.cfg.getConfigValue(config.SECTION_COMMON, config.STATEDIR, config.DEFAULT_STATEDIR)
    state_file = os.path.join(state_dir, lrms.getStateFile())
    
    state_data = None
    if os.path.exists(state_file):
        state_data = open(state_file).readline().strip() # state is only on the first line

    lrms.parseGeneratorState(state_data)

def writeGeneratorState(lrms):
    """
    Write the state of where the logs have been parsed to.
    """
    state_dir = lrms.cfg.getConfigValue(config.SECTION_COMMON, config.STATEDIR, config.DEFAULT_STATEDIR)
    state_file = os.path.join(state_dir, lrms.getStateFile())

    dirpath = os.path.dirname(state_file)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath, mode=0750)

    f = open(state_file, 'w')
    f.write(lrms.createGeneratorState())
    f.close()

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

def writeUr(ur,cfg):
    """
    Write ur to disk
    """    
    log_dir = cfg.getConfigValue(config.SECTION_COMMON, config.LOGDIR, config.DEFAULT_LOG_DIR)    
    ur_dir = os.path.join(log_dir, 'urs')
    if not os.path.exists(ur_dir):
        os.makedirs(ur_dir)
    ur_file = os.path.join(ur_dir, ur.record_id)
    ur.writeXML(ur_file)
    
    logging.info('Wrote usage record to %s' % ur_file)