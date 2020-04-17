# hither2 axiomatic documentation

### Preliminary definitions

Two Python functions `f1` and `f2` are said to produce the same result if the following code block does not raise an exception.

```python
try f1:
    r1 = f1()
    e1 = None
except Exception as err:
    r1 = None
    e1 = err

try f1:
    r2 = f1()
    e2 = None
except Exception as err:
    r2 = None
    e2 = err

# TODO: define same_result() and same_exception()

assert same_result(r1, r2)
assert same_exception(e1, e2)
```

### Equivalence of calling and running

Let `fun` be a hither2 function and `args` be a Python dict.
Then, in any hither2 context, the following two functions produce the same result, or the `run` function returns a hither2 exception.

```python
def call():
    return fun(args)

def run():
    return fun.run(args).wait()
```

