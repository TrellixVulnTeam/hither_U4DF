# Hither diagnostic tests

To run the diagnostic tests using pytest, first be sure you are in the root of the source directory:

```
cd hither
```

Run the basic tests (no containers and no servers):

```
pytest --pyargs hither -s
```

Include containerization tests (requires Docker)

```
pytest --pyargs hither -s --container
```

Test singularity

```
HITHER_USE_SINGULARITY=TRUE pytest --pyargs hither -s --container
```

Remote compute resource tests (requires Docker)

```
pytest --pyargs hither -s --remote
```