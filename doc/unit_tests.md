# Hither unit tests

Run basic unit tests:

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