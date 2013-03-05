=================================
Radio Sky Monitor Imaging Scripts
=================================

These scripts provide for imaging of LOFAR Radio Sky Monitor (RSM) data on the
Lisa cluster.

Dependencies
------------

* The `Offline` and `LofarFT` packages from the LOFAR software repository;
* The "special" version of `awimager` from `r22783` of the
  `LOFAR-Task3482-imager` branch of the LOFAR repository (until this branch is
  merged to trunk).
* `pyrap <https://code.google.com/p/pyrap/>`_;
* `NumPy <http://www.scipy.org/>`_.

Input Data
----------

Each unit of work consists of a pair of observations: a calibrator and a
target. The calibrator observation consists of a number *N* of subbands
directed towards a known calibrator source. This is immediately followed by
*N* * *M* target subbands, tiling out a large area of the sky around the
zenith with *M* beams.  Each of the calibrator subbands has a 1-to-M
relationship with the target subbands at the same frequency.

In recent observations *N* = 40 and *M* = 6, but this is potentially variable in
future.

A skymodel is required for both the calibrator and for each beam of the target
(*M* + 1 skymodels).

Output Data
-----------

As part of the processing, our *N* * *M* target subbands are aggregated into
groups of *X* subbands. Currently, *X* = 10, but this is potentially variable.

The required outputs are:

* Calibrated data for each calibrator subband (*N* MeasurementSets);
* Calibrated data for each target subband group (*N* * *M* / *X* MeasurementSets);
* Image data for each target subband group (*N* * *M* / *X* images).
* Logs from each of the processing steps.

Workflow
--------

The processing of each unit of work is carried out by the script
`imaging-multibeam.py`. It takes a single command line argument: a parset
which contains all the configuration information it needs. It performs the
following workflow:

#. All input data is copied from shared storage to appropriate scratch areas
   for processing.

#. A separate BBS process (`calibrate-stand-alone`, from the LOFAR imaging
   repository) is invoked for each subband of the calibrator observation.

#. The resulting instrument databases are clipped using the script
   `edit_parmdb.py <https://github.com/jdswinbank/edit-parmdb>`_.

#. The clipped instrument databases are transferred from each calibrator
   subband to each of its *M* corresponding target subbands using
   `parmexportcal` and `calibrate-stand-alone` (both from the LOFAR imaging
   repository).

#. The target subbands are combined in groups of size *X* using `NDPPP` (LOFAR
   imaging repository).

#. For each group:

   #. Phase-only calibration is performed using `calibrate-stand-alone`.

   #. Bad stations are identified and using `asciistats.py` and `statsplot.py`
      from the LOFAR repository and then stripped from the data.

   #. A limit is set on the length of the longest baseline to be included when
      imaging.

   #. A temporary, "dirty" image is constructed using `awimager` (LOFAR
      imaging repository trunk) and used to calculate the threshold to be used
      for cleaning the final image.

   #. A "mask" is constructed, based on the contents of the appropriate
      skymodel, using the `msss_mask.py` script included in this repository.

   #. The final image is constructed using `awimager` (from LOFAR repository
      branch `LOFAR-Task3482-imager`).

   #. Required metadata is added to the outpit image using `addImagingInfo`
      from the LOFAR imaging repository.

Supporting Scripts
------------------

For any given RSM run, a large number of work units will be produced
(typically 96 in a 24 hour period). Each of those is processed completely
independently according to the above procedure. The script `generate.py` takes
observation IDs for the calibrator and the target field, and the file name of
a template configuration parameterset, as command line arguments, and
generates a job suitable for submitting to the Lisa queue which will process
one work unit.

A small number of sources are eligible for use as calibrators, and the
skymodels for these have all been pre-calculated. However, each of the *M*
beams in the target requires a skymodel specific to its observation direction.
This skymodel can be generated on LOFAR CEP using the script `gsm.py`. The
script `skymodel.py`, included in this repository, takes a list of directories
containing target observations as command line arguments, and prints to
standard output the `gsm.py` invocation required to generate an appropriate
skymodel.
