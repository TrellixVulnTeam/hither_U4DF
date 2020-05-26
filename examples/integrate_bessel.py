# integrate_bessel.py

import hither as hi

@hi.function('integrate_bessel', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:dc57157d6316')
def integrate_bessel(v, a, b):
    # Definite integral of bessel function of first kind
    # of order v from a to b
    import scipy.integrate as integrate
    import scipy.special as special
    return integrate.quad(lambda x: special.jv(v, x), a, b)[0]