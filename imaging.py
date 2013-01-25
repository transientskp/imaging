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

# All temporary writes go to scratch space on the node.
scratch = os.getenv("TMPDIR")

def run_awimager(parset_filename, parset_keys, initscript=None):
    with patched_parset(parset_filename, parset_keys) as parset:
        run_process("awimager", parset, initscript=initscript)
    return parset_keys["image"]


def run_ndppp(parset_filename, parset_keys, initscript=None):
    with patched_parset(parset_filename, parset_keys) as parset:
        run_process("NDPPP", parset, initscript=initscript)
    return parset_keys["msout"]


def run_calibrate_standalone(parset_filename, input_ms, skymodel, initscript=None):
    run_process("calibrate-stand-alone", input_ms, parset_filename, skymodel, initscript=initscript)
    return input_ms


def find_bad_stations(msname, initscript=None):
    # Using scripts developed by Martinez & Pandey
    statsdir = os.path.join(scratch, "stats")
    run_process("asciistats.py", "-i", msname, "-r", statsdir, initscript=initscript)
    statsfile = os.path.join(statsdir, os.path.basename(msname) + ".stats")
    run_process("statsplot.py", "-i", statsfile, "-o", os.path.join(scratch, "stats"), initscript=initscript)
    bad_stations = []
    with open(os.path.join(scratch, "stats.tab"), "r") as f:
        for line in f:
            if line.strip()[0] == "#": continue
            if line.split()[-1] == "True":
                bad_stations.append(line.split()[1])
    return bad_stations


def strip_stations(msin, msout, stationlist):
    t = table(msin)
    if stationlist:
        output = t.query("""
            all(
                [ANTENNA1, ANTENNA2] not in
                [
                    select rowid() from ::ANTENNA where NAME in %s
                ]
            )
            """ % str(stationlist)
        )
    else:
        output = t
    # Is a deep copy really necessary here?
    output.copy(msout, deep=True)


def estimate_noise(msin, parset, maxbl, initscript=None):
    noise_image = mkdtemp(dir=scratch)

    # Default parameters -- hardcoded in Antonia's script
    nchan = 10
    niter = 0
    npix = 256
    operation = "image"
    stokes = "IQUV"
    box_size = 25

    awimager_parset = lofar.parameterset.parameterset(parset)
    cellsize = awimager_parset.getString("cellsize")
    robust = awimager_parset.getFloat("robust")
    wmax = awimager_parset.getFloat("wmax")
    wplanes = awimager_parset.getInt("wprojplanes")

    run_process(
        "awimager",
        "cellsize=%s" % (cellsize,),
        "data=CORRECTED_DATA",
        "ms=%s" % (msin,),
        "nchan=%d" % (nchan,),
        "niter=%d" % (niter,),
        "npix=%d" % (npix,),
        "operation=%s" % (operation,),
        "robust=%d" % (robust,),
        "select=\"sumsqr(UVW[:2])<%.1e\"" % (maxbl**2,),
        "stokes=%s" % (stokes,),
        "wmax=%f" % (wmax,),
        "wprojplanes=%d" % (wplanes,),
        "image=%s" % (noise_image,),
        initscript=initscript
    )

    t = table(noise_image)
    # Why is there a 3 in here? Are we calculating the noise in Stokes V?
    # (this is lifted from Antonia, which is lifted from George, ...!)
    noise = t.getcol('map')[0, 0, 3, npix/2-box_size:npix/2+box_size, npix/2-box_size:npix/2+box_size].std()
    t.close()
    return noise


def make_mask(msin, parset, skymodel, initscript=None):
    mask_image = mkdtemp(dir=scratch)
    mask_sourcedb = mkdtemp(dir=scratch)
    operation = "empty"

    awimager_parset = lofar.parameterset.parameterset(parset)
    cellsize = awimager_parset.getString("cellsize")
    npix = awimager_parset.getFloat("npix")
    stokes = awimager_parset.getString("stokes")

    run_process(
        "awimager",
        "cellsize=%s" % (cellsize,),
        "ms=%s" % (msin,),
        "npix=%d" % (npix,),
        "operation=%s" % (operation,),
        "image=%s" % (mask_image,),
        "stokes=%s" % (stokes,),
        initscript=initscript
    )
    run_process(
        "makesourcedb",
        "in=%s" % (skymodel,),
        "out=%s" % (mask_sourcedb,),
        "format=<",
        initscript=initscript
    )
    run_process(
        "python",
        "/home/jswinban/imaging/msss_mask.py",
        mask_image,
        mask_sourcedb,
        initscript=initscript
    )
    return mask_image


def get_parset_subset(parset, prefix):
    subset = parset.makeSubset(prefix + ".", "")
    fd, parset_name = mkstemp(dir=scratch)
    subset.writeFile(parset_name)
    return parset_name


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

    # Copy to scratch directory
    for ms_name in chain(ms_target, ms_cal):
        copytree(ms_name, os.path.join(scratch, os.path.basename(ms_name)))
    ms_target = [os.path.join(scratch, os.path.basename(ms)) for ms in ms_target]
    ms_cal = [os.path.join(scratch, os.path.basename(ms)) for ms in ms_cal]

    # We'll run as many simultaneous jobs as we have CPUs
    pool = ThreadPool(cpu_count())

    # Calibration of each calibrator subband
    calcal_parset = get_parset_subset(input_parset, "calcal.parset")
    calcal_initscript = input_parset.getString("calcal.initscript")
    def calibrate_calibrator(cal):
        source = table(cal).getcol("LOFAR_TARGET")['array'][0].lower().replace(' ', '')
        skymodel = os.path.join(
            input_parset.getString("skymodel_dir"),
            "%s.skymodel" % (source,)
        )
        run_calibrate_standalone(calcal_parset, cal, skymodel, initscript=calcal_initscript)
        # TODO: Do we need edit_parmdb.py?
    pool.map(calibrate_calibrator, ms_cal)

    # Transfer calibration solutions to targets
    transfer_parset = get_parset_subset(input_parset, "transfer.parset")
    transfer_skymodel = input_parset.getString("transfer.skymodel")
    transfer_initscript = input_parset.getString("transfer.initscript")
    def transfer_calibration(ms_pair):
        cal, target = ms_pair
        parmdb_name = mkdtemp(dir=scratch)
        run_process("parmexportcal", "in=%s/instrument/" % (cal,), "out=%s" % (parmdb_name,), initscript=transfer_initscript)
        run_process("calibrate-stand-alone", "--parmdb", parmdb_name, target, transfer_parset, transfer_skymodel, initscript=transfer_initscript)
    pool.map(transfer_calibration, zip(ms_cal, ms_target))

    # Combine with NDPPP
    combined_ms = os.path.join(scratch, "combined.MS")
    run_ndppp(get_parset_subset(input_parset, "combine.parset"),
        {
            "msin": str(ms_target),
            "msout": combined_ms
        },
        initscript=input_parset.getString("combine.initscript")
    )

    # Phase only calibration of combined target subbands
    run_calibrate_standalone(
        get_parset_subset(input_parset, "phaseonly.parset"),
        combined_ms,
        input_parset.getString("phaseonly.skymodel"),
        initscript=input_parset.getString("phaseonly.initscript")
    )

    # Strip bad stations.
    # Note that the combined, calibrated, stripped MS is one of our output
    # data products, so we save that with the name specified in the parset.
    bad_stations = find_bad_stations(combined_ms, initscript=input_parset.getString("badstations.initscript"))
    stripped_ms = input_parset.getString("output_ms")
    strip_stations(combined_ms, stripped_ms, bad_stations)

#    # Image
#    maxbl = input_parset.getFloat("awimager.maxbl")
#    aw_parset_name = get_parset_subset(input_parset, "awimager.parset")
#    threshold = input_parset.getFloat("awimager.noise_multiplier") * estimate_noise(stripped_ms, aw_parset_name, maxbl)
#    mask = make_mask(stripped_ms, aw_parset_name, skymodel)
#
#    print run_awimager(aw_parset_name,
#        {
#            "ms": stripped_ms,
#            "mask": mask,
#            "threshold": "%fJy" % (threshold,),
#            "select": "\"sumsqr(UVW[:2])<%.1e\"" % (maxbl**2,),
#            "image": input_parset.getString("output_im")
#        }
#    )
