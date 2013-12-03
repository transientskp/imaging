#!/usr/bin/env python

import os
import sys
import numpy
import math
import glob
import shutil
import tempfile
import lofar.parameterset

from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from tempfile import mkdtemp

from pyrap.tables import table

from utility import run_process
from utility import time_code
from utility import get_parset_subset
from utility import make_directory
from utility import copy_to_work_area

from utility import run_awimager
from utility import run_ndppp
from utility import run_calibrate_standalone
from utility import clear_calibrate_stand_alone_logs
from utility import find_bad_stations
from utility import strip_stations
from utility import limit_baselines
from utility import estimate_noise
from utility import sorted_ms_list
from utility import generate_skymodel

if __name__ == "__main__":
    # All temporary writes go to scratch space.
    # NB should clean up when we exit.
    scratch = tempfile.mkdtemp(dir=os.getenv("TMPDIR"))

    # Our single command line argument is a parset containing all
    # configuration information we'll need.
    input_parset = lofar.parameterset.parameterset(sys.argv[1])

    # We require `sbs_per_beam` input MeasurementSets for each beam, including
    # the calibrator.
    sbs_per_beam = sum(input_parset.getIntVector("band_size"))

    print "Locating calibrator data and checking paths"

    ms_cal = {}
    ms_cal["datafiles"] = sorted_ms_list(input_parset.getString("calibrator_input_glob"))
    assert(len(ms_cal["datafiles"]) == sbs_per_beam)
    ms_cal["output_dir"] = os.path.join(
        input_parset.getString("output_dir"),
        "calibrator",
        input_parset.getString("cal_obsid")
    )
    make_directory(ms_cal["output_dir"])

    print "Copying calibrator subbands to output"
    ms_cal["datafiles"] = copy_to_work_area(ms_cal["datafiles"], ms_cal["output_dir"])

    print "Locating target data and checking paths"
    # ms_target will be a dict that provides all the information we need to
    # process each independent element of the observation, where an "element"
    # is a combination of a beam (SAP) and a band (number of subbands)
    ms_target = {}

    # We need n_beams * sbs_per_beam MSs. If we have more than that, we trim
    # the array; if less, we'll trigger the assertion.
    target_mss = sorted_ms_list(input_parset.getString("target_input_glob"))
    target_mss = target_mss[:input_parset.getInt("n_beams") * sbs_per_beam]
    assert(len(target_mss) == input_parset.getInt("n_beams") * sbs_per_beam)

    for beam, data in enumerate(zip(*[iter(target_mss)]*sbs_per_beam)):
        start_sb = 0
        for band, band_size in enumerate(input_parset.getIntVector("band_size")):
            target_info = {}
            target_info['datafiles'] = target_mss[start_sb:start_sb+band_size]
            target_info['calfiles' ] = ms_cal["datafiles"][start_sb:start_sb+band_size]
            assert(len(target_info['datafiles']) == len(target_info['calfiles']))

            target_info['output_dir'] = os.path.join(
                input_parset.getString("output_dir"),
                "target",
                input_parset.getString("target_obsid"),
                "SAP00%d" % (beam,)
            )

            make_directory(target_info["output_dir"])

            target_info["output_ms"] = os.path.join(target_info["output_dir"], "%s_SAP00%d_band%d.MS" % (input_parset.getString("target_obsid"), beam, band))
            assert(not os.path.exists(target_info["output_ms"]))
            target_info["output_im"] = os.path.join(target_info["output_dir"], "%s_SAP00%d_band%d.img" % (input_parset.getString("target_obsid"), beam, band))
            assert(not os.path.exists(target_info["output_im"]))
            pointing = map(math.degrees, table("%s::FIELD" % target_info["datafiles"][0]).getcol("REFERENCE_DIR")[0][0])
            target_info["skymodel"] = os.path.join(
                scratch, "%.2f_%.2f.skymodel" % (pointing[0], pointing[1])
            )
            if not os.path.exists(target_info["skymodel"]):
                generate_skymodel(
                    input_parset.makeSubset("gsm."),
                    pointing[0], pointing[1],
                    target_info["skymodel"]
                )
            assert(os.path.exists(target_info["skymodel"]))
            ms_target["SAP00%d_band%d" % (beam, band)] = target_info
            start_sb += band_size

    # Copy to working directories
    for name in ms_target.iterkeys():
        print "Copying %s to scratch area" % (name,)
        ms_target[name]["datafiles"] = copy_to_work_area(
            ms_target[name]["datafiles"], scratch
        )

    # Limit the number of simultaneous jobs.
    nthreads = input_parset.getInt("calcal.nthreads")
    if nthreads == 0:
        nthreads = cpu_count()
    pool = ThreadPool(nthreads)

    # Calibration of each calibrator subband
    os.chdir(ms_cal['output_dir']) # Logs will get dumped here
    clear_calibrate_stand_alone_logs()
    calcal_parset = get_parset_subset(input_parset, "calcal.parset", scratch)
    def calibrate_calibrator(cal):
        source = table("%s::OBSERVATION" % (cal,)).getcol("LOFAR_TARGET")['array'][0].lower().replace(' ', '')
        skymodel = os.path.join(
            input_parset.getString("skymodel_dir"),
            "%s.skymodel" % (source,)
        )
        print "Calibrating %s with skymodel %s" % (cal, skymodel)
        run_calibrate_standalone(calcal_parset, cal, skymodel, replace_parmdb=True, replace_sourcedb=True)
    with time_code("Calibration of calibrator"):
        pool.map(calibrate_calibrator, ms_cal["datafiles"])

    # Clip calibrator parmdbs
    def clip_parmdb(sb):
        run_process(
            input_parset.getString("pdbclip.executable"),
            "--auto",
            "--sigma=%f" % (input_parset.getFloat("pdbclip.sigma"),),
            os.path.join(sb, "instrument")
        )
    with time_code("Clip calibrator instrument databases"):
        pool.map(lambda sb: clip_parmdb(sb), ms_cal["datafiles"])

    # Transfer calibration solutions to targets
    transfer_parset = get_parset_subset(input_parset, "transfer.parset", scratch)
    transfer_skymodel = input_parset.getString("transfer.skymodel")
    clear_calibrate_stand_alone_logs()
    def transfer_calibration(ms_pair):
        cal, target = ms_pair
        print "Transferring solution from %s to %s" % (cal, target)
        parmdb_name = mkdtemp(dir=scratch)
        run_process("parmexportcal", "in=%s/instrument/" % (cal,), "out=%s" % (parmdb_name,))
        run_process("calibrate-stand-alone", "--parmdb", parmdb_name, target, transfer_parset, transfer_skymodel)
    with time_code("Transfer of calibration solutions"):
        for target in ms_target.itervalues():
            pool.map(transfer_calibration, zip(target["calfiles"], target["datafiles"]))

    # Combine with NDPPP
    def combine_ms(target_info):
        output = os.path.join(mkdtemp(dir=scratch), "combined.MS")
        run_ndppp(
            get_parset_subset(input_parset, "combine.parset", scratch),
            {
                "msin": str(target_info["datafiles"]),
                "msout": output
            }
        )
        target_info["combined_ms"] = output
    with time_code("Combining target subbands"):
        pool.map(combine_ms, ms_target.values())


    # Phase only calibration of combined target subbands
    print "Running phase only calibration"
    def phaseonly(target_info):
        # We chdir to the scratch directory initially, so that logs get dumped
        # there, then we'll copy the logs to the output directory when we're
        # done.
        try:
            os.chdir(os.path.dirname(target_info["combined_ms"]))
            run_calibrate_standalone(
                get_parset_subset(input_parset, "phaseonly.parset", scratch),
                target_info["combined_ms"],
                target_info["skymodel"]
            )
            for logfile in glob.glob(
                os.path.join(
                    os.path.dirname(target_info["combined_ms"]),
                    "*log"
                )
            ):
                shutil.copy(logfile, target_info["output_dir"])
        except Exception, e:
            print "Error in phaseonly with %s" % (target_info["combined_ms"])
            print str(e)
            raise

    # Limit the number of simultaneous jobs.
    # Most Lisa nodes have 24 GB RAM -- we don't want to run out
    nthreads = input_parset.getInt("phaseonly.nthreads")
    if nthreads == 0:
        nthreads = cpu_count()
    calpool = ThreadPool(nthreads)
    with time_code("Phase-only calibration"):
        calpool.map(phaseonly, ms_target.values())

    # Strip bad stations.
    # Note that the combined, calibrated, stripped MS is one of our output
    # data products, so we save that with the name specified in the parset.
    def strip_bad_stations(target_info):
        bad_stations = find_bad_stations(target_info["combined_ms"], scratch)
        strip_stations(target_info["combined_ms"], target_info["output_ms"], bad_stations)
    with time_code("Strip bad stations"):
        pool.map(strip_bad_stations, ms_target.values())

    # Limit the length of the baselines we're using.
    # We'll image a reference table using only the short baselines.
    maxbl = input_parset.getFloat("limit.max_baseline")
    def limit_bl(target_info):
        target_info["bl_limit_ms"] = mkdtemp(dir=scratch)
        limit_baselines(target_info["output_ms"], target_info["bl_limit_ms"], maxbl)
    with time_code("Limiting maximum baseline length"):
        pool.map(limit_bl, ms_target.values())

    # Calculate the threshold for cleaning based on the noise in a dirty map
    # We don't use our threadpool here, since awimager is parallelized
    noise_parset_name = get_parset_subset(input_parset, "noise.parset", scratch)
    with time_code("Calculating threshold for cleaning"):
        for target_info in ms_target.values():
            print "Getting threshold for %s" % target_info["output_ms"]
            target_info["threshold"] = input_parset.getFloat("noise.multiplier") * estimate_noise(
                target_info["bl_limit_ms"],
                noise_parset_name,
                maxbl,
                input_parset.getFloat("noise.box_size"),
                scratch
            )
            print "Threshold for %s is %f Jy" % (target_info["output_ms"], target_info["threshold"])

    with time_code("Making images"):
        for target_info in ms_target.values():
            print "Making image %s" % target_info["output_im"]
            print run_awimager(aw_parset_name,
                {
                    "ms": target_info["bl_limit_ms"],
                    "threshold": "%fJy" % (target_info["threshold"],),
                    "image": target_info["output_im"],
                    "wmax": maxbl
                }
            )
            print "Updaging metadata in %s" % target_info["output_im"]
            run_process(
                "addImagingInfo",
                "%s.restored.corr" % target_info["output_im"],
                "", # No sky model specified
                "0",
                str(maxbl),
                target_info["output_ms"]
            )
            print "Saving mask for %s to %s" % (target_info["output_im"], target_info["output_im"] + ".mask")
