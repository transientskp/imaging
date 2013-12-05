import numpy
import cmath

class ComplexArray(object):
    def __init__(self):
        raise NotImplementedError

    def get_amp(self):
        return numpy.absolute(numpy.nan_to_num(self.data))
    def set_amp(self, new_amps):
        self.data = numpy.array(new_amps) * self.data / numpy.absolute(self.data)
    amp = property(get_amp, set_amp)
    Ampl = amp

    def get_phase(self):
        return numpy.angle(numpy.nan_to_num(self.data))
    def set_phase(self, new_phase):
        self.data = numpy.vectorize(cmath.rect)(
            numpy.absolute(self.data), numpy.array(new_phase)
        )
    phase = property(get_phase, set_phase)
    Phase = phase

    def get_real(self):
        return numpy.real(numpy.nan_to_num(self.data))
    def set_real(self, new_real):
        self.data = numpy.array(new_real) + 1j * numpy.imag(self.data)
    real = property(get_real, set_real)
    Real = real

    def get_imag(self):
        return numpy.imag(numpy.nan_to_num(self.data))
    def set_imag(self, new_imag):
        self.data = numpy.real(self.data) + 1j *  numpy.array(new_imag)
    imag = property(get_imag, set_imag)
    Imag = imag

    @property
    def writeable(self):
        return dict((key, getattr(self, key)) for key in self.keys)

class RealImagArray(ComplexArray):
    keys = ("Real", "Imag")
    def __init__(self, real, imag):
        self.data = numpy.array(real) + 1j * numpy.array(imag)

class AmplPhaseArray(ComplexArray):
    keys = ("Ampl", "Phase")
    def __init__(self, ampl, phase):
        self.data = numpy.vectorize(cmath.rect)(
            numpy.array(ampl), numpy.array(phase)
        )

ARRAY_TYPES = [RealImagArray, AmplPhaseArray]
