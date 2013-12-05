#!/usr/bin/env python

import sys
import numpy
from parmdb.stationgain import StationGain
from parmdb.utils import list_stations
from twisted.python import usage

class Options(usage.Options):
    optFlags = [
        ["auto", None, "Automatic mode"],
        ["last", None, "Include last value"]
    ]
    optParameters = [
        ["sigma", None, 3, "Clip at sigma * median", float],
        ["station", None, None, "Process this station"]
    ]

    def __init__(self):
        usage.Options.__init__(self)
        self['stations'] = set()

    def opt_station(self, station):
        self['stations'].add(station)

    def parseArgs(self, pdbfile):
        self['pdbfile' ] = pdbfile

def parse_options():
    """
Parse command line and recturn a populated instance of usage.Options.
"""
    config = Options()
    try:
        config.parseOptions()
        return config
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

def process_station(station, config):
    """
Process a station, clipping and plotting as specified.
"""
    for pol, data in station.iteritems():
        if not config['last']:
            amplitudes = data.amp[:-1]
            timescale = station.timescale[:-1] - station.timescale[0]
        else:
            amplitudes = data.amp
            timescale = station.timescale - station.timescale[0]
        median = numpy.median(amplitudes)
        stddev = numpy.std(amplitudes)
        corrected = numpy.where(
            numpy.abs(amplitudes-median) > config['sigma'] * stddev,
            median,
            amplitudes
        )

        def write_data():
            if not config['last']:
                data.amp = numpy.concatenate((corrected, data.amp[-1:]))
            else:
                data.amp = corrected
            station.writeout()

        if config['auto']:
            write_data()

        else:
            import matplotlib # Why is this necessary on heastro1?
            matplotlib.use("gtkagg")
            from matplotlib import pyplot as plt
            fig = plt.figure()
            fig.suptitle("%s:%s" % (station.station, pol))
            raw_axes = fig.add_subplot(2, 1, 1)
            corr_axes = fig.add_subplot(2, 1, 2)

            def keypress(event):
                # If the user presses 'q', quit immediately.
                # If the user presses 'w', write the data to the parmdb
                if event.key in ('q', 'Q'):
                    sys.exit(0)
                if event.key in ('w', 'W'):
                    write_data()

            cid = fig.canvas.mpl_connect('key_press_event', keypress)

            # Plot the raw data
            raw_axes.set_ylabel("Raw amplitude")
            raw_axes.plot(timescale, amplitudes, color='b', marker='.', ls='')
            bad_positions = numpy.where(
                numpy.abs(amplitudes-median) > config['sigma'] * stddev
            )[0]
            raw_axes.plot(
                timescale[bad_positions],
                amplitudes[bad_positions],
                color='r', marker='x', ls=''
            )
            raw_axes.axhline(median, color='r')
            raw_axes.axhline(median+config["sigma"]*stddev, color='g', ls='--')
            raw_axes.axhline(median+stddev, color='y', ls='--')
            raw_axes.axhline(median-config["sigma"]*stddev, color='g', ls='--')
            raw_axes.axhline(median-stddev, color='y', ls='--')
            raw_axes.axhline(median-config["sigma"]*stddev, color='g', ls='--')

            # Then the corrected data
            corr_axes = fig.add_subplot(2, 1, 2)
            corr_axes.set_ylabel("Corrected amplitude")
            corr_axes.set_xlabel("Time [s]")
# corr_axes.set_ylim(median-stddev-1, median+stddev+1)
            corr_axes.plot(timescale, corrected, color='b', marker='.', ls='')
            corr_axes.axhline(median, color='r')
            corr_axes.axhline(median+stddev, color='y', ls='--')
            corr_axes.axhline(median+config["sigma"]*stddev, color='g', ls='--')
            corr_axes.axhline(median-stddev, color='y', ls='--')
            corr_axes.axhline(median-config["sigma"]*stddev, color='g', ls='--')

            print "\nNow plotting %s:%s" % (station.station, pol)
            print "Press 'w' to write corrected data to parmdb."
            print "Press 'q' to quit."
            print "Close the plot to continue."
            plt.show()

if __name__ == "__main__":
    config = parse_options()

    if not config['stations']:
        stations = list_stations(config['pdbfile'])
    else:
        stations = sorted(config['stations'])

    for station in stations:
        print "Processing station %s" % (station,)
        station = StationGain(config['pdbfile'], station)
        process_station(station, config)
