#!/usr/bin/python

# fill a mask based on skymodel
# Usage: ./msss_mask.py mask-file skymodel
# Example: ./msss_mask.py wenss-2048-15.mask skymodel.dat
# Bugs: fdg@mpa-garching.mpg.de

# version 0.2+
#
# Edited by JDS, 2012-03-16:
# * Properly convert maj/minor axes to half length
# * Handle empty fields in sky model by setting them to 0
# * Fix off-by-one error at mask boundary
#
# FIXED BUG
# * if a source is outside the mask, the script ignores it
# * if a source is on the border, the script draws only the inner part
# * can handle skymodels with different headers
#
# KNOWN BUG
# * not works with single line skymodels, workaround: add a fake source outside the field

# 
# Version 0.3  (Wouter Klijn, klijn@astron.nl)
# * Usage of sourcedb instead of txt document as 'source' of sources
#   This allows input from different source sources 
#

import pyrap.images as pi
import sys
import numpy as np
import os
import random
import re
import pyrap.tables as pt
import lofar.parmdb

pad = 500. # increment in maj/min axes [arcsec]

# open command line arguments
mask_file = sys.argv[1]
catalogue = sys.argv[2]


# open mask
mask = pi.image(mask_file, overwrite = True)
mask_data = mask.getdata()
xlen, ylen = mask.shape()[2:]
freq, stokes, null, null = mask.toworld([0, 0, 0, 0])


#Open the sourcedb:
table = pt.table(catalogue + "::SOURCES")
pdb = lofar.parmdb.parmdb(catalogue)

# Get the data of interest
source_list = table.getcol("SOURCENAME")
source_type = table.getcol("SOURCETYPE")
all_values_dict = pdb.getDefValues()  # All date in the format valuetype:sourcename

# Loop the sources
for source, type in zip(source_list, source_type):
    if type == 1:
        type_string = "Gaussian"
    else:
        type_string = "Point"
    print "processing: {0} ({1})".format(source, type_string)

    # Get de ra and dec (already in radians)
    ra = all_values_dict["Ra:" + source][0, 0]
    dec = all_values_dict["Dec:" + source][0, 0]
    if type == 1:
        # Get the raw values from the db
        maj_raw = all_values_dict["MajorAxis:" + source][0, 0]
        min_raw = all_values_dict["MinorAxis:" + source][0, 0]
        pa_raw = all_values_dict["Orientation:" + source][0, 0]
        #convert to radians (conversion is copy paste JDS)
        maj = (((maj_raw + pad)) / 3600.) * np.pi / 180. # major radius (+pad) in rad
        min = (((min_raw + pad)) / 3600.) * np.pi / 180. # minor radius (+pad) in rad
        pa = pa_raw * np.pi / 180.
        if maj == 0 or min == 0: # wenss writes always 'GAUSSIAN' even for point sources -> set to wenss beam+pad
            maj = ((54. + pad) / 3600.) * np.pi / 180.
            min = ((54. + pad) / 3600.) * np.pi / 180.
    elif type == 0: # set to wenss beam+pad
        maj = (((54. + pad) / 2.) / 3600.) * np.pi / 180.
        min = (((54. + pad) / 2.) / 3600.) * np.pi / 180.
        pa = 0.
    else:
        print "WARNING: unknown source type ({0}), ignoring it.".format(type)
        continue

    #print "Maj = ", maj*180*3600/np.pi, " - Min = ", min*180*3600/np.pi # DEBUG

    # define a small square around the source to look for it
    null, null, y1, x1 = mask.topixel([freq, stokes, dec - maj, ra - maj / np.cos(dec - maj)])
    null, null, y2, x2 = mask.topixel([freq, stokes, dec + maj, ra + maj / np.cos(dec + maj)])
    xmin = np.int(np.floor(np.min([x1, x2])))
    xmax = np.int(np.ceil(np.max([x1, x2])))
    ymin = np.int(np.floor(np.min([y1, y2])))
    ymax = np.int(np.ceil(np.max([y1, y2])))

    if xmin > xlen or ymin > ylen or xmax < 0 or ymax < 0:
        print "WARNING: source ", source, "falls outside the mask, ignoring it."
        continue

    if xmax > xlen or ymax > ylen or xmin < 0 or ymin < 0:
        print "WARNING: source ", source, "falls across map edge."


    for x in xrange(xmin, xmax):
        for y in xrange(ymin, ymax):
            # skip pixels outside the mask field
            if x >= xlen or y >= ylen or x < 0 or y < 0:
                continue
            # get pixel ra and dec in rad
            null, null, pix_dec, pix_ra = mask.toworld([0, 0, y, x])

            X = (pix_ra - ra) * np.sin(pa) + (pix_dec - dec) * np.cos(pa); # Translate and rotate coords.
            Y = -(pix_ra - ra) * np.cos(pa) + (pix_dec - dec) * np.sin(pa); # to align with ellipse
            if X ** 2 / maj ** 2 + Y ** 2 / min ** 2 < 1:
                mask_data[0, 0, y, x] = 1

mask.putdata(mask_data)
table.close()
