#!/usr/bin/env python

import os
import sys
import textwrap
import lofar.parameterset

from utility import make_directory
from utility import get_file_list

# Settings for December 2012 test observation
N_BEAMS = 6
BAND_SIZE = [12, 11, 11]
INPUT_DIR = "/home/jswinban/RSM_TEST_DEC2012"
OUTPUT_DIR = "/home/jswinban/RSM_output/TEST_DEC2012-calremote"
SKYMODEL_DIR = "/home/jswinban/imaging/skymodels"

TEMPLATE_JOB = """
    #PBS -lwalltime=6:00:00
                             # 6 hours wall-clock
                             # time allowed for this job
    #PBS -lnodes=1:ppn=8
                             # 1 node for this job
    #PBS -S /bin/bash
    source /home/jswinban/sw/init.sh
    source /home/jswinban/sw/lofim/lofarinit.sh
    cd %s
    time python /home/jswinban/imaging/imaging-multibeam.py %s
"""
TEMPLATE_JOB = textwrap.dedent(TEMPLATE_JOB).strip()

if __name__ == "__main__":
    target_obsid = sys.argv[1]
    cal_obsid = sys.argv[2]
    template_parset = sys.argv[3]

    # Check data exists: we should have sum(BAND_SIZE) subbands in each beam,
    # N_BEAMS beams per target_obsid, and 1 beam per cal_obsid
    assert(len(get_file_list(INPUT_DIR, cal_obsid, 0)) == sum(BAND_SIZE))
    for beam in range(N_BEAMS):
        assert(len(get_file_list(INPUT_DIR, target_obsid, beam)) == sum(BAND_SIZE))

    CAL_OUTPUT = os.path.join(OUTPUT_DIR, "calibrator", cal_obsid)
    TARGET_OUTPUT = os.path.join(OUTPUT_DIR, "target", target_obsid)
    make_directory(CAL_OUTPUT)
    make_directory(TARGET_OUTPUT)

    parset = lofar.parameterset.parameterset(template_parset)
    parset.replace("input_dir", INPUT_DIR)
    parset.replace("cal_obsid", cal_obsid)
    parset.replace("target_obsid", target_obsid)
    parset.replace("n_beams", str(N_BEAMS))
    parset.replace("band_size", str(BAND_SIZE))
    parset.replace("output_dir", OUTPUT_DIR)
    parset.replace("skymodel_dir", SKYMODEL_DIR)
    parset_filename = os.path.join(TARGET_OUTPUT, target_obsid + ".parset")
    parset.writeFile(parset_filename)

    job = TEMPLATE_JOB % (TARGET_OUTPUT, parset_filename)
    with open(os.path.join(TARGET_OUTPUT, target_obsid + ".job"), "w") as jobfile:
        jobfile.write(job)
