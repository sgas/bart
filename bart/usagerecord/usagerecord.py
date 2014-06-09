#
# Usage Record representation and generation.
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
# Some code copied from the arc-ur-logger in ARC.
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009, 2010)

import time
from bart import __version__

from bart.usagerecord import urelements as ur

try:
    from xml.etree import ElementTree as ET
except ImportError:
    # Python 2.4 compatability
    from elementtree import ElementTree as ET


# constant / defaults
ISO_TIME_FORMAT    = "%Y-%m-%dT%H:%M:%S"
XML_HEADER         = '''<?xml version="1.0" encoding="UTF-8" ?>''' + "\n"

# values for the logger name + version
LOGGER_NAME_VALUE    = 'SGAS-BaRT'
LOGGER_VERSION_VALUE = __version__

# register namespaces in element tree so we get more readable xml files
# the semantics of the xml files does not change due to this
try:
    register_namespace = ET.register_namespace
except AttributeError:
    def register_namespace(prefix, uri):
        ET._namespace_map[uri] = prefix

register_namespace('ur', ur.OGF_UR_NAMESPACE)
register_namespace('deisa', ur.DEISA_NAMESPACE)
register_namespace('vo', ur.SGAS_VO_NAMESPACE)
register_namespace('sgas', ur.SGAS_UR_NAMESPACE)
register_namespace('logger', ur.LOGGER_NAMESPACE)



class VOInformation:

    def __init__(self, name=None, type_=None, issuer=None):
        self.name = name
        self.type_ = type_
        self.issuer = issuer
        self.attributes = [] # [group, role, capability]



class UsageRecord:

    def __init__(self):
        self.record_id          = None
        self.global_job_id      = None
        self.local_job_id       = None
        self.global_user_name   = None
        self.local_user_id      = None
        self.job_name           = None
        self.status             = None
        self.machine_name       = None
        self.queue              = None
        self.host               = None
        self.node_count         = None
        self.processors         = None
        self.submit_time        = None
        self.end_time           = None
        self.start_time         = None
        self.project_name       = None
        self.submit_host        = None
        self.wall_duration      = None
        self.cpu_duration       = None
        self.charge             = None
        self.vo_info            = [] # list of VOInformation
        # sgas attributes
        self.user_time          = None
        self.kernel_time        = None
        self.exit_code          = None
        self.major_page_faults  = None
        self.runtime_environments = []
        # logger attributes
        self.logger_name        = ur.LOGGER_NAME
        self.logger_version     = ur.LOGGER_VERSION


    def generateTree(self):
        """
        Generates the XML tree for usage record.
        """

        # utility function, very handy
        def setElement(parent, name, text):
            element = ET.SubElement(parent, name)
            element.text = str(text)

        # begin method

        ure = ET.Element(ur.JOB_USAGE_RECORD)

        assert self.record_id is not None, "No recordId specified, cannot generate usage record"
        record_identity = ET.SubElement(ure, ur.RECORD_IDENTITY)
        record_identity.set(ur.RECORD_ID, self.record_id)
        record_identity.set(ur.CREATE_TIME, time.strftime(ISO_TIME_FORMAT, time.gmtime()) + 'Z')

        if self.global_job_id is not None or self.local_job_id is not None:
            job_identity = ET.SubElement(ure, ur.JOB_IDENTITY)
            if self.global_job_id is not None:
                setElement(job_identity, ur.GLOBAL_JOB_ID, self.global_job_id)
            if self.local_job_id is not None:
                setElement(job_identity, ur.LOCAL_JOB_ID, self.local_job_id)

        if self.global_user_name is not None or self.local_job_id is not None:
            user_identity = ET.SubElement(ure, ur.USER_IDENTITY)
            if self.local_user_id is not None:
                setElement(user_identity, ur.LOCAL_USER_ID, self.local_user_id)
            if self.global_user_name is not None:
                setElement(user_identity, ur.GLOBAL_USER_NAME, self.global_user_name)

            # vo stuff belongs under user identity
            for voi in self.vo_info:

                vo = ET.SubElement(user_identity, ur.VO)
                if voi.type_ is not None:
                    vo.attrib[ur.VO_TYPE] = voi.type_
                setElement(vo, ur.VO_NAME, voi.name)
                if voi.issuer is not None:
                    setElement(vo, ur.VO_ISSUER, voi.issuer)

                for attrs in voi.attributes:
                    group, role, capability = attrs
                    attr = ET.SubElement(vo, ur.VO_ATTRIBUTE)
                    setElement(attr, ur.VO_GROUP, group)
                    if role is not None:
                        setElement(attr, ur.VO_ROLE, role)
                    if capability is not None:
                        setElement(attr, ur.VO_CAPABILITY, capability)

        if self.job_name       is not None :  setElement(ure, ur.JOB_NAME, self.job_name)
        if self.charge         is not None :  setElement(ure, ur.CHARGE, self.charge)
        if self.status         is not None :  setElement(ure, ur.STATUS, self.status)
        if self.machine_name   is not None :  setElement(ure, ur.MACHINE_NAME, self.machine_name)
        if self.queue          is not None :  setElement(ure, ur.QUEUE, self.queue)
        if self.host           is not None :  setElement(ure, ur.HOST, self.host)
        if self.node_count     is not None :  setElement(ure, ur.NODE_COUNT, self.node_count)
        if self.processors     is not None :  setElement(ure, ur.PROCESSORS, self.processors)
        if self.submit_host    is not None :  setElement(ure, ur.SUBMIT_HOST, self.submit_host)
        if self.project_name   is not None :  setElement(ure, ur.PROJECT_NAME, self.project_name)
        if self.submit_time    is not None :  setElement(ure, ur.SUBMIT_TIME, self.submit_time)
        if self.start_time     is not None :  setElement(ure, ur.START_TIME, self.start_time)
        if self.end_time       is not None :  setElement(ure, ur.END_TIME, self.end_time)
        if self.wall_duration  is not None :  setElement(ure, ur.WALL_DURATION, "PT%fS" % self.wall_duration)
        if self.cpu_duration   is not None :  setElement(ure, ur.CPU_DURATION, "PT%fS" % self.cpu_duration)
        # sgas attributes
        if self.user_time      is not None :  setElement(ure, ur.USER_TIME, "PT%fS" % self.user_time)
        if self.kernel_time    is not None :  setElement(ure, ur.KERNEL_TIME, "PT%fS" % self.kernel_time)
        if self.exit_code      is not None :  setElement(ure, ur.EXIT_CODE, self.exit_code)
        if self.major_page_faults is not None :
            setElement(ure, ur.MAJOR_PAGE_FAULTS, self.major_page_faults)
        for renv in self.runtime_environments:
            setElement(ure, ur.RUNTIME_ENVIRONMENT, renv)

        # set logger name and version
        logger_name = ET.SubElement(ure, ur.LOGGER_NAME)
        logger_name.text = LOGGER_NAME_VALUE
        logger_name.set(ur.LOGGER_VERSION, LOGGER_VERSION_VALUE)

        return ET.ElementTree(ure)


    def writeXML(self, filename):
        tree = self.generateTree()
        f = file(filename, 'w')
        f.write(XML_HEADER)
        tree.write(f, encoding='utf-8')


# ----

def gm2isoTime(gm_time):
    return time.strftime(ISO_TIME_FORMAT, gm_time) + "Z"


def epoch2isoTime(epoch_time):
    gmt = time.gmtime(epoch_time)
    return gm2isoTime(gmt)

