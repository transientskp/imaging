import re
import os
import time
import errno
import warnings
import subprocess
import lofar.parameterset
import lofar.gsm.gsmutils
import monetdb.sql as db
from glob import glob
from contextlib import contextmanager, closing
from tempfile import mkstemp, mkdtemp
from shutil import copytree, rmtree
from pyrap.tables import table


def generate_skymodel(gsm_parset, ra, dec, output_filename):
    """
    Connect to the GSM database at the location specified. Extract a skymodel
    file. Save it to the specified location.
    """
    with closing(
        db.connect(
            hostname=gsm_parset.getString("db_host"),
            database=gsm_parset.getString("db_dbase"),
            username=gsm_parset.getString("db_user"),
            password=gsm_parset.getString("db_passwd"),
            port=gsm_parset.getInt("db_port"),
            autocommit=gsm_parset.getBool("db_autocommit")
        )
    ) as conn:
        lofar.gsm.gsmutils.expected_fluxes_in_fov(
            conn, ra, dec,
            gsm_parset.getFloat("radius"),
            gsm_parset.getFloat("assoc_theta"),
            output_filename,
            vlss_flux_cutoff=gsm_parset.getFloat("vlss_cutoff")
        )
    return output_filename


def read_ms_list(filename):
    """
    Read a list of MeasurementSets stored one per line in `filename`. If all
    MSs exist, return the list; if one or more are missing, raise.
    """
    with open(filename, 'r') as f:
        ms_list = [l.strip() for l in f.readlines()]
    for ms in ms_list:
        assert(os.path.exists(ms))
    return ms_list


def sorted_ms_list(dirname, suffix="*_uv.MS.dppp"):
    """
    Return a list of all MSs (or files with names ending with `suffix`) in
    `dirname`, sorted according to subband number.
    """
    return sorted(
        glob(os.path.join(dirname, suffix)),
        key=lambda ms: re.search(r"SB(\d{3})", ms).groups()[0]
    )


def get_file_list(root_dir, obsid, beam):
    return sorted(
        glob(os.path.join(
            root_dir, obsid, "%s_SAP00%d_SB*_uv.MS.dppp" % (obsid, beam)
        ))
    )


@contextmanager
def time_code(name):
    start_time = time.time()
    try:
        yield
    finally:
        print "%s took %f seconds" % (name, time.time() - start_time)


def get_parset_subset(parset, prefix, scratchdir):
    subset = parset.makeSubset(prefix + ".", "")
    fd, parset_name = mkstemp(dir=scratchdir)
    subset.writeFile(parset_name)
    return parset_name


def patch_parset(parset_filename, data, output_dir=None):
    temp_parset = lofar.parameterset.parameterset(parset_filename)
    for key, value in data.iteritems():
        temp_parset.replace(key, str(value))
    fd, output = mkstemp(dir=output_dir)
    temp_parset.writeFile(output)
    os.close(fd)
    return output


@contextmanager
def patched_parset(parset_filename, data, output_dir=None, unlink=True):
    filename = patch_parset(parset_filename, data, output_dir)
    try:
        yield filename
    finally:
        if unlink: os.unlink(filename)


def make_directory(path):
    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise


def copy_to_work_area(input_file_list, work_area):
    outputs = []
    for ms_name in input_file_list:
        output_name = os.path.join(work_area, os.path.basename(ms_name))
        if os.path.exists(output_name):
            rmtree(output_name)
        copytree(ms_name, output_name)
        outputs.append(output_name)
    return outputs


def run_process(executable, *args, **kwargs):
    args = list(args)
    args.insert(0, executable)
    env = None
    if "initscript" in kwargs and kwargs["initscript"]:
        env = read_initscript(kwargs['initscript'])
        if "module" in env:
            del env['module']
    print "Executing: " + " ".join(args)
    subprocess.check_call(args, env=env)


def read_initscript(filename, shell="/bin/sh"):
    if not os.path.exists(filename):
        warnings.warn("Initialization script %s not found" % (filename,))
        return {}
    else:
        print "Reading environment from %s" % (filename,)
        p = subprocess.Popen(
            ['. %s ; env' % (filename,)],
            shell=True,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
        )
        so, se = p.communicate()
        environment = [x.split('=', 1) for x in so.strip().split('\n')]
        environment = filter(lambda x: len(x) == 2, environment)
        return dict(environment)


def run_awimager(parset_filename, parset_keys, initscript=None):
    with patched_parset(parset_filename, parset_keys) as parset:
        run_process("awimager", parset, initscript=initscript)
    return parset_keys["image"]


def run_ndppp(parset_filename, parset_keys, initscript=None):
    with patched_parset(parset_filename, parset_keys) as parset:
        run_process("NDPPP", parset, initscript=initscript)
    return parset_keys["msout"]


def run_calibrate_standalone(parset_filename, input_ms, skymodel, replace_parmdb=False, replace_sourcedb=False, initscript=None):
    args = ["calibrate-stand-alone", input_ms, parset_filename, skymodel]
    kwargs = {}
    if initscript:
        kwargs["initscript"] = initscript
    if replace_parmdb:
        args.insert(1, "--replace-parmdb")
    if replace_sourcedb:
        args.insert(1, "--replace-sourcedb")
    run_process(*args, **kwargs)
    return input_ms


def clear_calibrate_stand_alone_logs(directory=None):
    # calibrate-stand-alone will create a log file according to the pattern
    # calibrate-stand-alone_${PID}.log in the current working dir. If that
    # file already exists, it will die. We rename old log files to ensure they
    # can't cause it to break.
    if not directory:
        directory = os.getcwd()
    for logfile in glob(os.path.join(directory, "calibrate-stand-alone*log")):
        os.rename(logfile, logfile + ".old")


def find_bad_stations(msname, scratchdir, initscript=None):
    # Using scripts developed by Martinez & Pandey
    statsdir = os.path.join(mkdtemp(dir=scratchdir))
    run_process("asciistats.py", "-i", msname, "-r", statsdir, initscript=initscript)
    statsfile = os.path.join(statsdir, os.path.basename(msname) + ".stats")
    output_basename = os.path.join(statsdir, "stats")
    run_process("statsplot.py", "-i", statsfile, "-o", output_basename, initscript=initscript)
    bad_stations = []
    with open(output_basename + ".tab", "r") as f:
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


def limit_baselines(msin, msout, maxbl):
    t = table(msin)
    out = t.query("sumsqr(UVW[:2])<%.1e" % (maxbl**2,))
    out.copy(msout, deep=False)


def estimate_noise(msin, parset_name, wmax, box_size, scratchdir, awim_init=None):
    noise_image = mkdtemp(dir=scratchdir)

    run_awimager(parset_name,
        {
            "ms": msin,
            "image": noise_image,
            "wmax": wmax
        },
        initscript=awim_init
    )

    parset = lofar.parameterset.parameterset(parset_name)
    npix = parset.getFloat("npix")
    t = table(noise_image)
    # Why is there a 3 in here? Are we calculating the noise in Stokes V?
    # (this is lifted from Antonia, which is lifted from George, ...!)
    noise = t.getcol('map')[0, 0, 3, npix/2-box_size:npix/2+box_size, npix/2-box_size:npix/2+box_size].std()
    t.close()
    return noise


def make_mask(msin, parset, skymodel, executable, scratchdir, awim_init=None):
    mask_image = mkdtemp(dir=scratchdir)
    mask_sourcedb = mkdtemp(dir=scratchdir)
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
        initscript=awim_init
    )
    run_process(
        "makesourcedb",
        "in=%s" % (skymodel,),
        "out=%s" % (mask_sourcedb,),
        "format=<"
    )
    run_process(
        executable,
        mask_image,
        mask_sourcedb
    )
    return mask_image
