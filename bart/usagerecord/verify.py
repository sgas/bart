#
# Usage Record verification.
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Magnus Jonsson <magnus@hpc2n.umu.se>
# Copyright: NeIC 2014


from bart.usagerecord import urparser

from twisted.python import log

def verify(ur):
    try:
        d = urparser.xmlToDict(ur)
        if d['record_id'] is None:
            log.err("No record_id found")
            return False
    except:
        log.err("Failed to convert UR XML into dict")
        return False
    return True

