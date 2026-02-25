try:
    pass
    # __all__ = ["plot_memory_sessions", "MemoryMonitor"]
except ImportError:
    # Dependencies not installed → provide dummy / helpful error
    raise RuntimeError(
        "Developer tools not available. Install with: pip install 'tadween-core[dev]'"
    )
