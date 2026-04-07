try:
    import pandas  # noqa: F401
except ImportError:
    raise RuntimeError(
        "Developer tools not available. Install dev dependencies with: `uv sync --group dev`."
        "`devtools` package is only available from source code and not available from package manager."
    )
