import time
import uuid
from pathlib import Path

import requests
from pydantic import BaseModel

from .base import BaseHandler


class DownloadInput(BaseModel):
    url: str
    suffix: str = ""
    timeout_seconds: int = 300


class DownloadOutput(BaseModel):
    local_path: str
    size_bytes: int
    download_duration_seconds: float


class DownloadHandler(BaseHandler[DownloadInput, DownloadOutput]):
    """Downloads a remote file to local storage."""

    def __init__(self, local_dir: str, max_retries: int = 3):
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries

    def run(self, inputs: DownloadInput) -> DownloadOutput:

        local_path = self.local_dir / f"{uuid.uuid4()}{inputs.suffix}"

        start_time = time.time()
        for attempt in range(self.max_retries):
            try:
                self._download_file(inputs.url, local_path, inputs.timeout_seconds)
                break
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2**attempt)

        return DownloadOutput(
            local_path=str(local_path),
            size_bytes=local_path.stat().st_size,
            download_duration_seconds=time.time() - start_time,
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
