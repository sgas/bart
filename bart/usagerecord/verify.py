#
# Usage Record verification.
#
# Module for the SGAS Batch system Reporting Tool (BaRT).
#
# Author: Magnus Jonsson <magnus@hpc2n.umu.se>
# Copyright: NeIC 2014


from bart.usagerecord import urparser

def verify(ur):
    try:
        d = urparser.xmlToDict(ur)
        if d['record_id'] is None:
            return False
    except:
        return False
    return True

