import os
from typing import Literal, TypeAlias

FileMode: TypeAlias = Literal["overwrite", "new", "append"]


def reserve_path(log_dir: str, filename: str, mode: FileMode = "new") -> str:
    """
    Reserve a path for logging based on the mode.

    Args:
        log_dir: Directory for log files
        filename: Desired filename
        mode: One of "new", "overwrite", or "append"
            - "new": Create a new file with _N suffix if file exists (default)
            - "overwrite": Remove existing file and create fresh
            - "append": Return existing file path or create if doesn't exist

    Returns the absolute path that has been reserved.
    """
    os.makedirs(log_dir, exist_ok=True)
    base_name, ext = os.path.splitext(filename)
    desired = os.path.join(log_dir, filename)

    if mode == "overwrite":
        try:
            os.remove(desired)
        except FileNotFoundError:
            pass
        # Create empty file to reserve it
        fd = os.open(desired, os.O_CREAT | os.O_WRONLY)
        os.close(fd)
        return desired

    elif mode == "append":
        if os.path.exists(desired):
            return desired
        fd = os.open(desired, os.O_CREAT | os.O_WRONLY)
        os.close(fd)
        return desired

    elif mode == "new":
        # Try to create filename, if exists try _1, _2, ...
        candidate = filename
        i = 1
        while True:
            candidate_path = os.path.join(log_dir, candidate)
            try:
                fd = os.open(candidate_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return candidate_path
            except FileExistsError:
                candidate = f"{base_name}_{i}{ext}"
                i += 1
    else:
        raise ValueError(f"Unrecognizable file mode: {mode}")


def format_mb(n_bytes) -> float:
    return round(n_bytes / 1024 / 1024, 2)
