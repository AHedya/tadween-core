try:
    import polars  # noqa: F401
except ImportError:
    raise RuntimeError(
        "Developer tools not available. Install with: pip install 'tadween-core[dev]'"
    )
