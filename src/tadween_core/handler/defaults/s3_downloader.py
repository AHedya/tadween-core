import time
from pathlib import Path

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config
    from pydantic import BaseModel

except ImportError:
    raise ImportError("boto3 is not installed. install s3 extension `tadween-core[s3]`")

from ..base import BaseHandler


class S3DownloadInput(BaseModel):
    bucket: str
    key: str
    suffix: str = ""
    local_path: Path | None = None


class S3DownloadOutput(BaseModel):
    local_path: Path
    size_bytes: int
    download_duration_seconds: float


class S3DownloadHandler(BaseHandler[S3DownloadInput, S3DownloadOutput]):
    def __init__(
        self,
        local_dir: str,
        *,
        access_key: str,
        secret_key: str,
        endpoint_url: str | None = None,
        session_token: str | None = None,
        max_retries: int = 3,
        multipart_threshold_mb: int = 64,
        max_workers: int = 4,
        max_concurrency_per_file: int = 4,
    ):
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)

        self._client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            endpoint_url=endpoint_url,
            config=Config(
                retries={"mode": "adaptive", "max_attempts": max_retries},
                max_pool_connections=max_workers * max_concurrency_per_file + 4,
            ),
        )

        self._transfer_cfg = TransferConfig(
            multipart_threshold=multipart_threshold_mb * 1024 * 1024,
            multipart_chunksize=16 * 1024 * 1024,
            max_concurrency=max_concurrency_per_file,
            use_threads=True,
        )

    def run(self, inputs: S3DownloadInput) -> S3DownloadOutput:
        local_path = Path(
            inputs.local_path or self.local_dir / f"{inputs.key}{inputs.suffix}"
        )

        start_time = time.perf_counter()
        self._download(inputs.bucket, inputs.key, local_path)

        return S3DownloadOutput(
            local_path=local_path,
            size_bytes=local_path.stat().st_size,
            download_duration_seconds=round(time.perf_counter() - start_time, 4),
        )

    def _download(self, bucket: str, key: str, dest: Path) -> None:
        tmp = dest.with_suffix(".tmp")
        try:
            self._client.download_file(bucket, key, str(tmp), Config=self._transfer_cfg)
            tmp.rename(dest)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

    def shutdown(self):
        self._client.close()

    def warmup(self):
        pass
