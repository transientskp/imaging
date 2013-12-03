import os
import lofar.parameterset

class parameterset(lofar.parameterset.parameterset):
    def getString(self, keyname, subst={}):
        raw_str = super(parameterset, self).getString(keyname)
        return raw_str.format(**subst)
