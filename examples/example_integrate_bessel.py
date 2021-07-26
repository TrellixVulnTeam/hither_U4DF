import hither2 as hi
from integrate_bessel import integrate_bessel

# call function directly
val1 = integrate_bessel(v=2.5, a=0, b=4.5)

# call using hither pipeline
job: hi.Job = integrate_bessel.run(v=2.5, a=0, b=4.5)
val2 = job.wait().return_value

# run inside container
with hi.Config(use_container=True):
    job: hi.Job = integrate_bessel.run(v=2.5, a=0, b=4.5)
    val3 = job.wait().return_value

print(val1, val2, val3)
