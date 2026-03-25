import time
import uuid
from pathlib import Path

import requests
from pydantic import BaseModel

from ..base import BaseHandler


class DownloadInput(BaseModel):
    url: str
    suffix: str = ""
    timeout_seconds: int = 300
    local_path: Path | None = None
    retries: int | None = None


class DownloadOutput(BaseModel):
    local_path: str
    size_bytes: int
    download_duration_seconds: float


class DownloadHandler(BaseHandler[DownloadInput, DownloadOutput]):
    """Downloads a remote file to local storage."""

    def __init__(
        self, local_dir: str, max_retries: int = 3, request_timeout: int = 120
    ):
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.timeout = request_timeout
        # TODO: Make retryable errors configurabe via handler config.
        self._RETRYABLE_STATUSES = {429, 408, 500, 502, 503, 504}
        self._RETRYABLE_NETWORK_EXCEPTIONS = (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        )

    def run(self, inputs: DownloadInput) -> DownloadOutput:
        timeout = inputs.timeout_seconds or self.timeout
        max_retries = inputs.retries if inputs.retries is not None else self.max_retries
        local_path = Path(
            inputs.local_path or self.local_dir / f"{uuid.uuid4()}{inputs.suffix}"
        )

        start_time = time.perf_counter()
        for attempt in range(max_retries):
            try:
                self._download_file(inputs.url, local_path, timeout)
                break
            except Exception as exc:
                if not self._is_retryable(exc):
                    raise
                last_exc = exc
                time.sleep(2**attempt)
        else:
            raise RuntimeError(
                f"Download failed after {max_retries} attempts: {inputs.url}"
            ) from last_exc

        return DownloadOutput(
            local_path=str(local_path),
            size_bytes=local_path.stat().st_size,
            download_duration_seconds=time.perf_counter() - start_time,
        )

    def _download_file(self, url: str, dest: Path, timeout: int):
        tmp = dest.with_suffix(".tmp")
        try:
            response = requests.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            tmp.rename(dest)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

    def _is_retryable(self, exc: Exception) -> bool:
        if isinstance(exc, requests.exceptions.HTTPError):
            status = exc.response.status_code if exc.response is not None else None
            return status in self._RETRYABLE_STATUSES
        return isinstance(exc, self._RETRYABLE_NETWORK_EXCEPTIONS)
