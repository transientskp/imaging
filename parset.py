import os
import lofar.parameterset

class parameterset(lofar.parameterset.parameterset):
    def getPath(self, keyname):
        raw_str = super(parameterset, self).getString(keyname)
        return raw_str.format(cwd = os.getcwd())
