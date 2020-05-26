import hither as hi
from integrate_bessel import integrate_bessel

# call function directly
val1 = integrate_bessel(v=2.5, a=0, b=4.5)

# call using hither pipeline
job = integrate_bessel.run(v=2.5, a=0, b=4.5)
val2 = job.wait()

# run inside container
with hi.Config(container=True):
    job = integrate_bessel.run(v=2.5, a=0, b=4.5)
    val3 = job.wait()

print(val1, val2, val3)
