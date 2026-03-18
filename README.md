**Under heavy construction, do not use for serious work!**

## Muxpack

Muxpack is a Python implementation for working with multiplex network files.

## Documentation

Build docs locally using the same dependency path as Read the Docs:

```bash
uv sync --group docs
uv run sphinx-build -b html docs docs/_build/html
```

The generated HTML is available in `docs/_build/html/index.html`.