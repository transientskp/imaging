#!/usr/bin/env python

import os
import sys
import numpy
import lofar.parameterset

from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from shutil import copytree
from itertools import chain
from tempfile import mkstemp, mkdtemp

from pyrap.tables import table

from utility import patched_parset
from utility import run_process
from utility import time_code
from utility import get_parset_subset

from utility import run_awimager
from utility import run_ndppp
from utility import run_calibrate_standalone
from utility import find_bad_stations
from utility import strip_stations
from utility import limit_baselines
from utility import estimate_noise
from utility import make_mask

# All temporary writes go to scratch space on the node.
scratch = os.getenv("TMPDIR")

if __name__ == "__main__":
    # Our single command line argument is a parset containing all
    # configuration information we'll need.
    input_parset = lofar.parameterset.parameterset(sys.argv[1])

    # Change to appropriate working directory for logs, etc.
    os.chdir(input_parset.getString("working_dir"))

    # Lists of input files.
    ms_target = input_parset.getStringVector("ms_target")
    ms_cal = input_parset.getStringVector("ms_cal")
    assert(len(ms_target) == len(ms_cal))

    # Check that all our inputs exist
    for msname in chain(ms_target, ms_cal):
        assert(os.path.exists(msname))

    # Check that output files don't exist before starting
    output_dir = input_parset.getString("output_directory")
    output_ms = os.path.join(output_dir, input_parset.getString("output_ms"))
    output_im = os.path.join(output_dir, input_parset.getString("output_im"))
    assert(not os.path.exists(output_ms))
    assert(not os.path.exists(output_im))

    # Copy to working directories
    for ms_name in ms_target:
        copytree(ms_name, os.path.join(scratch, os.path.basename(ms_name)))
    ms_target = [os.path.join(scratch, os.path.basename(ms)) for ms in ms_target]
    for ms_name in ms_cal:
        copytree(ms_name, os.path.join(output_dir, os.path.basename(ms_name)))
    ms_cal = [os.path.join(output_dir, os.path.basename(ms)) for ms in ms_cal]

    # We'll run as many simultaneous jobs as we have CPUs
    pool = ThreadPool(cpu_count())

    # Calibration of each calibrator subband
    calcal_parset = get_parset_subset(input_parset, "calcal.parset")
    def calibrate_calibrator(cal):
        source = table("%s::OBSERVATION" % (cal,)).getcol("LOFAR_TARGET")['array'][0].lower().replace(' ', '')
        skymodel = os.path.join(
            input_parset.getString("skymodel_dir"),
            "%s.skymodel" % (source,)
        )
        print "Calibrating %s with skymodel %s" % (cal, skymodel)
        run_calibrate_standalone(calcal_parset, cal, skymodel)
    with time_code("Calibration of calibrator"):
        pool.map(calibrate_calibrator, ms_cal)

    # Clip calibrator parmdbs
    def clip_parmdb(sb):
        run_process(
            input_parset.getString("pdbclip.executable"),
            "--auto",
            "--sigma=%f" % (input_parset.getFloat("pdbclip.sigma"),),
            os.path.join(sb, "instrument")
        )
    with time_code("Clip calibrator instrument databases"):
        pool.map(lambda sb: clip_parmdb(sb), ms_cal)

    # Transfer calibration solutions to targets
    transfer_parset = get_parset_subset(input_parset, "transfer.parset")
    transfer_skymodel = input_parset.getString("transfer.skymodel")
    def transfer_calibration(ms_pair):
        cal, target = ms_pair
        parmdb_name = mkdtemp(dir=scratch)
        run_process("parmexportcal", "in=%s/instrument/" % (cal,), "out=%s" % (parmdb_name,))
        run_process("calibrate-stand-alone", "--parmdb", parmdb_name, target, transfer_parset, transfer_skymodel)
    with time_code("Transfer of calibration solutions"):
        pool.map(transfer_calibration, zip(ms_cal, ms_target))

    # Combine with NDPPP
    combined_ms = os.path.join(scratch, "combined.MS")
    with time_code("Combination of subbands using NDPPP"):
        run_ndppp(get_parset_subset(input_parset, "combine.parset"),
            {
                "msin": str(ms_target),
                "msout": combined_ms
            }
        )

    # Phase only calibration of combined target subbands
    target_skymodel = input_parset.getString("phaseonly.skymodel")
    with time_code("Phase-only calibration"):
        run_calibrate_standalone(
            get_parset_subset(input_parset, "phaseonly.parset"),
            combined_ms,
            target_skymodel
        )

    # Strip bad stations.
    # Note that the combined, calibrated, stripped MS is one of our output
    # data products, so we save that with the name specified in the parset.
    with time_code("Strip bad stations"):
        bad_stations = find_bad_stations(combined_ms, scratch)
        stripped_ms = output_ms
        strip_stations(combined_ms, stripped_ms, bad_stations)

    # Limit the length of the baselines we're using.
    # We'll image a reference table using only the short baselines.
    maxbl = input_parset.getFloat("limit.max_baseline")
    with time_code("Limiting maximum baseline length"):
        bl_limit_ms = mkdtemp(dir=scratch)
        limit_baselines(stripped_ms, bl_limit_ms, maxbl)

    # We source a special build for using the "new" awimager
    awim_init = input_parset.getString("awimager.initscript")

    # Calculate the threshold for cleaning based on the noise in a dirty map
    noise_parset_name = get_parset_subset(input_parset, "noise.parset")
    with time_code("Estimating noise"):
        threshold = input_parset.getFloat("noise.multiplier") * estimate_noise(
            bl_limit_ms,
            noise_parset_name,
            maxbl,
            input_parset.getFloat("noise.box_size")
        )

    # Make a mask for cleaning
    aw_parset_name = get_parset_subset(input_parset, "image.parset")
    with time_code("Making mask"):
        mask = make_mask(
            stripped_ms,
            aw_parset_name,
            target_skymodel,
            input_parset.getString("make_mask.executable"),
            awim_init=awim_init
        )

    with time_code("Making image"):
        print run_awimager(aw_parset_name,
            {
                "ms": bl_limit_ms,
                "mask": mask,
                "threshold": "%fJy" % (threshold,),
                "image": output_im,
                "wmax": maxbl
            },
            initscript=awim_init
        )
