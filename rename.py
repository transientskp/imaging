#!/usr/bin/env python

# Rename MeasurementSets to be filed by ObsID
#
# Reads a list of observations as provided by Science Support in the following
# format. This should be stored in the file "observations.txt" in the current
# working directory:
#
# L107728  LC0_003   BEAM0     HBA_DUAL_INNER   HBA_110_190   2013-03-24 23:56:54 -- 2013-03-25 00:07:54
# L107727  LC0_003  3C 295     HBA_DUAL_INNER   HBA_110_190   2013-03-24 23:53:54 -- 2013-03-24 23:55:54
# L107726  LC0_003   BEAM0     HBA_DUAL_INNER   HBA_110_190   2013-03-24 23:41:54 -- 2013-03-24 23:52:54
#
# Walks the tree with root INPUT_ROOT, finds MeasurementSets and moves them to
# be named according to the pattern:
#
# OUTPUT_ROOT/LXXXXX/LXXXXX_SAPYYY_SBZZZ_uv.dppp.MS
#
# Where LXXXXX is the *obsid* as given in the observations.txt file.
#
# Customize INPUT_ROOT, OUTPUT_ROOT and BEAM_EDGES, below to control the
# output.

from __future__ import division
import pyrap.tables as pt
import datetime
import os
import re
import sys
import shutil
import pytz

INPUT_ROOT = "/home/jswinban/RSM_run2_sorted"
OUTPUT_ROOT = "/home/jswinban/RSM_run2"
BEAM_EDGES = [40, 80, 120, 160, 200, 240, 244]

julian_epoch = datetime.datetime(1858, 11, 17)
unix_epoch = datetime.datetime(1970, 1, 1, 0, 0)
delta = unix_epoch - julian_epoch
delta_seconds = (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 10**6) / 10**6

def mjds_to_unix(mjds):
    return mjds - delta_seconds

def sb_to_sap(sb):
   return len([x for x in BEAM_EDGES if x <= sb])

obs_mapping = {}
with open("observations.txt", "r") as obs_file:
    for line in obs_file:
        obsid = line.split()[0]
        date = datetime.datetime.strptime(line[60:80].strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.utc)
        obs_mapping[date] = obsid

for (ms, dirnames, filenames) in os.walk(INPUT_ROOT, topdown=False):
    if ms[-2:] == "MS":
        t = pt.table("%s::OBSERVATION" % (ms,))
        obs_date = datetime.datetime.fromtimestamp(mjds_to_unix(t.getcol("TIME_RANGE")[0][0]), pytz.utc)
        obsid = obs_mapping[obs_date]
        obs_subband = re.search(r"SB([0-9]{3})", ms).groups()[0]
        obs_sap = sb_to_sap(int(obs_subband))
        output_dir = os.path.join(OUTPUT_ROOT, obsid)
        target = os.path.join(output_dir, "%s_SAP00%d_SB%s_uv.dppp.MS" % (obsid, obs_sap, obs_subband))
        try:
            os.makedirs(output_dir)
        except:
            pass
        if os.path.abspath(ms) != os.path.abspath(target):
            shutil.move(ms, target)
            print "Moved %s to %s" % (ms, target)
