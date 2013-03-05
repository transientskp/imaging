=================================
Radio Sky Monitor Imaging Scripts
=================================

These scripts provide for imaging of LOFAR Radio Sky Monitor (RSM) data on the
Lisa cluster.

Input Data
----------

Each unit of work consists of a pair of observations: a calibrator and a
target. The calibrator observation consists of a number *N* of subbands directed
towards a known calibrator source. This is immediately followed by *N* * *M*
subbands, tiling out a large area of the sky around the zenith with M beams.
Each of the calibrator subbands has a 1-to-M relationship with the target
subbands at the same frequency.

In recent observations *N* = 40 and *M* = 6, but this is potentially variable in
future.

Output Data
-----------

As part of the processing, our *N* * *M* target subbands are aggregated into
groups of X subbands. Currently, *X* = 10, but this is potentially variable.

The required outputs are:

* Calibrated data for each calibrator subband (*N* MeasurementSets);
* Calibrated data for each target subband group (*N* * *M* / *X* MeasurementSets);
* Image data for each target subband group (*N* * *M* / *X* images).
