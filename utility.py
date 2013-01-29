import os
import time
import warnings
import subprocess
import lofar.parameterset
from contextlib import contextmanager
from tempfile import mkstemp

@contextmanager
def time_code(name):
    start_time = time.time()
    try:
        yield
    finally:
        print "%s took %f seconds" % (name, time.time() - start_time)


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


def run_process(executable, *args, **kwargs):
    args = list(args)
    args.insert(0, executable)
    env = None
    if "initscript" in kwargs and kwargs["initscript"]:
        env = read_initscript(kwargs['initscript'])
        if "module" in env:
            del env['module']
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
