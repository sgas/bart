#!/usr/bin/python
#
# SGE log parser and UR generator
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Erik Edelmann <edelmann@csc.fi>
# Copyright: Nordic e-Infrastructure Collaboration (2019)

import os
import time
import logging
from datetime import datetime

from bart import common
from bart.usagerecord import usagerecord

ACCOUNTING_FILE = './acc'
UR_DIR = 'urs'

def parse_accounting_file(fname):
    with open(ACCOUNTING_FILE, "r") as f:
        for line in f.readlines():
            if line[0] == '#': continue

            fields = line.split(':')
            job = {
                'qname':           fields[0],
                'hostname':        fields[1],
                'group':           fields[2],
                'owner':           fields[3],
                'job_name':        fields[4],
                'job_number':      fields[5],
                'account':         fields[6],
                'priority':        fields[7],
                'submit_time':     datetime.fromtimestamp(float(fields[8])),
                'start_time':      datetime.fromtimestamp(float(fields[9])),
                'end_time':        datetime.fromtimestamp(float(fields[10])),
                'failed':          fields[11],
                'exit_status':     fields[12],
                'project':         fields[31],
                'slots':           fields[34],
                'cpu_sec':         float(fields[36]),
                'maxvmem':         fields[42]
            }

            yield job

def ur_from_job(machine_name, job):

    voi = usagerecord.VOInformation()
    voi.type = 'lrmsurgen-projectmap'
    voi.name = 'alice'
    
    ur = usagerecord.UsageRecord()
    ur.record_id        = machine_name + ':' + job['start_time'].strftime("%s") + ':' + job['job_number']
    ur.local_job_id     = job['job_number']
    ur.global_job_id    = machine_name + ':' + job['job_number']
    ur.job_name         = job['job_name']
    ur.local_user_id    = job['owner']
    ur.global_user_name = job['owner']
    ur.machine_name     = machine_name
    ur.queue            = job['qname']
    ur.processors       = job['slots']
    ur.node_count       = 1
    ur.host             = job['hostname']
    ur.submit_time      = job['submit_time'].isoformat()
    ur.start_time       = job['start_time'].isoformat()
    ur.end_time         = job['end_time'].isoformat()
    ur.cpu_duration     = job['cpu_sec']
    ur.wall_duration    = (job['end_time'] - job['start_time']).total_seconds()
    ur.project_name     = job['project']
    ur.vo_info         += [voi]

    return ur




for job in parse_accounting_file(ACCOUNTING_FILE):
    print job['end_time'].strftime("%Y-%m-%d %H:%M:%S")
    ur = ur_from_job('alice-tron.hip.fi', job)
    ur_file = os.path.join(UR_DIR, ur.record_id)
    ur.writeXML(ur_file)

