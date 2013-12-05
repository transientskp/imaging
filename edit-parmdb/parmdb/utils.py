#!/usr/bin/env python

from .parmdb import WriteableParmDB

def list_stations(pdbfile):
    """
    Returns a list of all stations in the parmdb.
    """
    try:
        pdb = WriteableParmDB(pdbfile)
        return sorted(set(name.split(":")[-1] for name in pdb.getNames()))
    finally:
        pdb = None
