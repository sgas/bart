#
# Configration and parsing module.
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Author: Magnus Jonsson <magnus@hpc2n.umu.se>
# Copyright: Nordic Data Grid Facility (2009, 2010)

import re
import ConfigParser
import pprint
from optparse import OptionParser

DEFAULT_CONFIG_FILE     = '/etc/bart/bart.conf'
DEFAULT_USERMAP_FILE    = '/etc/bart/usermap'
DEFAULT_VOMAP_FILE      = '/etc/bart/vomap'
DEFAULT_LOG_FILE        = '/var/log/bart-logger.log'
DEFAULT_LOG_DIR         = '/var/spool/bart/usagerecords'
DEFAULT_STATEDIR        = '/var/spool/bart'
DEFAULT_SUPPRESS_USERMAP_INFO = 'false'

# Common section
SECTION_COMMON = 'common'

HOSTNAME   = 'hostname'
USERMAP    = 'usermap'
VOMAP      = 'vomap'
LOGDIR     = 'logdir'
LOGFILE    = 'logfile'
STATEDIR   = 'statedir'
SUPPRESS_USERMAP_INFO = 'suppress_usermap_info'

# regular expression for matching mapping lines
rx = re.compile('''\s*(.*)\s*"(.*)"''')

def getParser():
    parser = OptionParser()
    parser.add_option('-l', '--log-file', dest='logfile', help='Log file (overwrites config option).')
    parser.add_option('-c', '--config', dest='config', help='Configuration file.',
                      default=DEFAULT_CONFIG_FILE, metavar='FILE')
    return parser

class BartConfig:
    def __init__(self,config_file):
        self.cfg = ConfigParser.ConfigParser()
        self.cfg.read(config_file)

    def getConfigValue(self, section, value, default=None):
        try:
            return self.cfg.get(section, value)
        except ConfigParser.NoSectionError:
            return default
        except ConfigParser.NoOptionError:
            return default

    def sections(self):
        return self.cfg.sections()

    def getConfigValueBool(self, section, value, default=None):
        value = self.getConfigValue(section, value, default);
        if value.lower() in ('true', 'yes', '1'):
            return True
        elif value.lower() in ('false', 'no', '0'):
            return False
        else:
            logging.error('Invalid option for % (%)' % (value, idtimestamp))

        if default is None:
            return False
        
        if default.lower() in ('true', 'yes', '1'):
            return True
        elif default.lower() in ('false', 'no', '0'):
            return False

        return False;
       
    # Check for missing items and check syntax
    def validate(self,section,lrms):
        print lrms
            
        for key in lrms.CONFIG:
            item = lrms.CONFIG[key]
            value = self.getConfigValue(section, key, None) 
            if value == None:
                if item['required'] == True:
                    print "Required Value '%s' is missing in section [%s]" % (key,section)
                    return False
            else:
                # check if value is of correct type                
                if 'type' in item and item['type'] in ['bool'] and value not in ['true','false','0','1','yes','no']:
                    print "Item '%s' defined as 'bool' in section [%s] does not have a valid syntax" % (key,section)
                    return False
                
                if 'type' in item and item['type'] in ['int']:
                    try:
                            int(value)
                    except ValueError:                    
                        print "Item '%s' defined as 'bool' in section [%s] does not have a valid syntax" % (key,section)
                        return False
                                    
        # Check for items not corresponding with the section
        for item in self.cfg.items(section):
            if item[0] not in lrms.CONFIG:
                print "Value '%s' found but not defiend in section [%s]" % (item[0] + "=" + item[1], section)
                return False                 
                
        return True

class BartMapFile:
    def __init__(self):
        self.map_ = {}

    def load(self,map_file):
        self.map_ = {}

        for line in open(map_file).readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            m = rx.match(line)
            if not m:
                continue
            key, mapped_value = m.groups()
            key = key.strip()
            mapped_value = mapped_value.strip()
            if mapped_value == '-':
                mapped_value = None
                
            self.map_[key] = mapped_value

        return self

    def getMapping(self):
        return self.map_
    
    def get(self,key):
        try:
            return self.map_[key]
        except KeyError:
            return None


