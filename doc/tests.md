# Hither diagnostic tests

To run the diagnostic tests using pytest, first be sure you are in the root of the source directory:

```bash
cd hither
```

Run the basic tests (no containers and no servers):

```bash
pytest --pyargs hither -s
```

Include containerization tests (requires Docker)

```bash
pytest --pyargs hither -s --container
```

Test singularity

```bash
HITHER_USE_SINGULARITY=TRUE pytest --pyargs hither -s --container
```

Remote compute resource tests (requires Docker)

```bash
pytest --pyargs hither -s --remote
```
