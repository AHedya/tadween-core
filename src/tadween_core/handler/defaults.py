import hashlib
import time
from pathlib import Path
from typing import Literal

import requests
from pydantic import BaseModel, Field

from .base import BaseHandler


class DownloadInput(BaseModel):
    url: str
    checksum: str | None = None
    timeout_seconds: int = 300


class DownloadOutput(BaseModel):
    local_path: str
    size_bytes: int
    checksum: str
    download_duration_seconds: float
    status: Literal["downloaded", "cached", "failed"] = Field(
        description="'downloaded', 'cached', or 'failed'"
    )


class DownloadHandler(BaseHandler[DownloadInput, DownloadOutput]):
    """Downloads remote files to local storage."""

    def __init__(self, local_dir: str, max_retries: int = 3):
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries

    def run(self, inputs: DownloadInput | dict) -> DownloadOutput:
        # Generate local filename
        if isinstance(inputs, dict):
            inputs = DownloadInput.model_validate(inputs)

        url_hash = hashlib.md5(inputs.url.encode()).hexdigest()
        local_path = self.local_dir / f"{url_hash}.audio"

        # Check if already downloaded
        if local_path.exists():
            cached_checksum = self._compute_checksum(local_path)
            if inputs.checksum is None or cached_checksum == inputs.checksum:
                return DownloadOutput(
                    local_path=str(local_path),
                    size_bytes=local_path.stat().st_size,
                    checksum=cached_checksum,
                    download_duration_seconds=0.0,
                    status="cached",
                )

        # Download with retries
        start_time = time.time()
        for attempt in range(self.max_retries):
            try:
                self._download_file(inputs.url, local_path, inputs.timeout_seconds)
                break
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2**attempt)  # Exponential backoff

        # Compute checksum
        checksum = self._compute_checksum(local_path)

        # Verify checksum if provided
        if inputs.checksum and checksum != inputs.checksum:
            local_path.unlink()
            raise ValueError(
                f"Checksum mismatch: expected {inputs.checksum}, got {checksum}"
            )

        duration = time.time() - start_time

        return DownloadOutput(
            local_path=str(local_path),
            size_bytes=local_path.stat().st_size,
            checksum=checksum,
            download_duration_seconds=duration,
            status="downloaded",
        )

    def _download_file(self, url: str, dest: Path, timeout: int):
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    def _compute_checksum(self, path: Path) -> str:
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
