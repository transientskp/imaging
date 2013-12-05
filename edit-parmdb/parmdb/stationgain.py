from complexarray import ARRAY_TYPES
from parmdb import WriteableParmDB

class StationGain(dict):
    def __init__(self, parmdbfile, station):
        self.station = station
        self.parmdbfile = parmdbfile

        pdb = WriteableParmDB(self.parmdbfile)

        names = pdb.getNames("Gain:*:*:*:%s" % (station,))
        pols = set(":".join(x[1:3]) for x in  (x.split(":") for x in names))
        types = set(x[3] for x in  (x.split(":") for x in names))

        for array_type in ARRAY_TYPES:
            if sorted(types) == sorted(array_type.keys):
                self.array_type = array_type
                break
        assert(hasattr(self, "array_type"))

        for polarization in pols:
            data = []
            for key in self.array_type.keys:
                query = "Gain:%s:%s:%s" % (polarization, key, station)
                data.append(pdb.getValuesGrid(query)[query])

            self.timescale = data[0]['times']
            self.timestep = data[0]['timewidths'][0]

            self.freqscale = data[0]['freqs']
            self.freqstep = data[0]['freqwidths'][0]

            self[polarization] = self.array_type(data[0]["values"], data[1]["values"])

        pdb = None

    def writeout(self):
        pdb = WriteableParmDB(self.parmdbfile)
        for pol, data in self.iteritems():
            for component, value in data.writeable.iteritems():
                pdb.setValues(
                    "Gain:%s:%s:%s" % (pol, component, self.station),
                    value,
                    self.freqscale[0],
                    self.freqstep,
                    self.timescale[0],
                    self.timestep
                )
        pdb = None
