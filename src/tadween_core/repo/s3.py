try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config
    from botocore.exceptions import (
        ClientError,
        EndpointResolutionError,
        NoCredentialsError,
    )

except ImportError:
    raise ImportError("Can't' import boto3. install s3 extension `tadween-core[s3]`")
import contextlib
import gzip
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from tadween_core.exceptions import S3InitError

from .base import ART, BaseArtifactRepo, PartNameT

MB = 1024 * 1024

_CT_JSON = "application/json"
_CT_BLOB = "application/octet-stream "


class S3ClientConfig(BaseModel):
    access_key: str = Field(serialization_alias="aws_access_key_id")
    secret_key: str = Field(serialization_alias="aws_secret_access_key")
    session_token: str | None = Field(
        default=None, serialization_alias="aws_session_token"
    )

    endpoint_url: str | None = None
    region: str = Field(default="us-east-1", serialization_alias="region_name")

    config: Config = Config(
        signature_version="s3v4",
        retries={"max_attempts": 3, "mode": "adaptive"},
        max_pool_connections=20,
    )

    def boto_kwargs(self) -> dict:
        return self.model_dump(by_alias=True, exclude_none=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class S3Repo(BaseArtifactRepo[ART, PartNameT]):
    def __init__(
        self,
        artifact_type: type[ART],
        bucket_id: str,
        prefix: str,
        boto_client: Any | None = None,
        client_config: S3ClientConfig | None = None,
        compress_threshold_bytes: int = 1 * MB,
        multipart_threshold_bytes: int = 5 * MB,
    ):
        """
        Args:
            artifact_type (type[ART]): Artifact type
            bucket_id (str): Bucket name
            prefix (str): Repository prefix (batch_id)
            boto_client (Any | None, optional): Injected boto S3 client. Defaults to None.
            client_config (S3ClientConfig | None, optional): S3 client config. Defaults to None.
            compress_threshold_bytes (int, optional): compress threshold. Any item greater value will be compressed. Defaults to 1*MB.
            multipart_threshold_bytes (int, optional): Triggers multipart mechanism. Item size must be greater than the value .Defaults to 5*MB.
        Raises:
            ValueError: When neither of `boto_client` nor `client_config` is passed. Or prefix is not a valid string
        """
        if boto_client is None and client_config is None:
            raise ValueError(
                "Can't have both the client and config set to `None`. Pass either of them"
            )
        if not prefix:
            raise ValueError("Prefix can't be None nor empty")

        super().__init__(artifact_type)

        self._client = boto_client or boto3.client("s3", client_config.boto_kwargs())
        self._bucket_id = bucket_id
        self._prefix = prefix.rstrip("/")
        self._compress_threshold = compress_threshold_bytes
        self._multipart_threshold = multipart_threshold_bytes

        preflight_check(self._client, self._bucket_id, True, self.logger, self._prefix)

        self._pool = ThreadPoolExecutor(
            max_workers=self._client.meta.config.max_pool_connections,
            thread_name_prefix="S3Repo-",
        )
        self._TRANSFER_SINGLE = TransferConfig(
            multipart_threshold=self._multipart_threshold,
            use_threads=False,
        )
        self._TRANSFER_LARGE = TransferConfig(
            multipart_threshold=self._multipart_threshold,
            max_concurrency=2,
            use_threads=True,
        )

    def save_many(self, artifacts, include="all", wait=True) -> None:
        include = self._resolve_batch_part_names(include, len(artifacts))

        tasks: list[tuple[str, bytes, str]] = []  # (key, body, content_type)
        for art, parts_included in zip(artifacts, include, strict=True):
            tasks.append(
                (self._get_root_key(art.id), self._serialize_root(art), _CT_JSON)
            )
            for part in parts_included:
                val = getattr(art, part)
                if val is None:
                    self.logger.warning(f"{art.id}: part [{part}] is None — skipping.")
                    continue
                tasks.append(
                    (self._get_part_key(art.id, part), val.serialize(), _CT_BLOB)
                )

        futures = [
            self._pool.submit(self._put, key, body, ct) for key, body, ct in tasks
        ]
        if wait:
            for f in futures:
                f.result()
            return None
        return futures

    def save(
        self,
        artifact,
        include="all",
    ):
        include = self._resolve_part_names(include)
        self.save_many(
            [artifact],
            include=[include],
        )
        return

    def save_part(self, artifact_id, part_name, data) -> None:
        if part_name not in self._part_map:
            raise ValueError(f"Unknown part {part_name}.")
        if not self.exists(artifact_id):
            raise KeyError(f"Artifact {artifact_id} does not exist.")
        expected = self._part_map[part_name]
        if not isinstance(data, expected):
            raise TypeError(f"Expected {expected.__name__}, got {type(data).__name__}.")
        self._put(
            self._get_part_key(artifact_id, part_name), data.serialize(), _CT_BLOB
        )

    def load_many(self, artifact_ids, include=None, **options) -> dict[str, ART]:
        raw_results = self.load_many_raw(artifact_ids, include=include, **options)

        result = {}
        for aid, raw in raw_results.items():
            if raw is None:
                result[aid] = None
                continue

            combined = self._artifact_type.model_validate_json(raw["root"])
            for part_name, part_raw in raw.items():
                if part_raw is None or part_name == "root":
                    continue
                part_value = self._part_map[part_name].deserialize(part_raw)
                # skip re-validation
                object.__setattr__(combined, part_name, part_value)
                combined.__pydantic_fields_set__.add(part_name)

            result[aid] = combined

        return result

    def load(self, artifact_id, include=None, **options):
        include = self._resolve_part_names(include)
        return self.load_many([artifact_id], include=[include], **options).get(
            artifact_id
        )

    def load_many_raw(
        self, artifact_ids, include=None, **options
    ) -> dict[str, dict[str, Any] | None]:
        include = self._resolve_batch_part_names(include, len(artifact_ids))

        tasks: list[tuple[str, str, str | None]] = []
        for aid, inc in zip(artifact_ids, include, strict=True):
            tasks.append((aid, self._get_root_key(aid), None))
            for part in inc:
                tasks.append((aid, self._get_part_key(aid, part), part))

        fetched: dict[tuple[str, str | None], str | None] = {}
        futures = {
            self._pool.submit(self._get, key): (aid, part) for aid, key, part in tasks
        }
        for fut, (aid, part) in futures.items():
            fetched[(aid, part)] = fut.result()

        result = {}
        for aid, inc in zip(artifact_ids, include, strict=True):
            raw_model = {}
            root_raw = fetched.get((aid, None))
            if root_raw is None:
                result[aid] = None
                continue
            raw_model["root"] = root_raw
            for part in inc:
                part_raw = fetched.get((aid, part))
                raw_model[part] = part_raw if part_raw is not None else None
            result[aid] = raw_model

        return result

    def load_raw(self, artifact_id, include=None, **options):
        include = self._resolve_part_names(include)
        return self.load_many_raw([artifact_id], include=[include], **options).get(
            artifact_id
        )

    def load_part(self, artifact_id, part_name):
        if part_name not in self._part_map:
            raise ValueError(f"Unknown part {part_name}.")
        if not self.exists(artifact_id):
            raise KeyError(f"Artifact {artifact_id} does not exist.")
        raw = self._get(self._get_part_key(artifact_id, part_name))
        if raw is None:
            return None
        return self._part_map[part_name].deserialize(raw)

    def delete_parts(self, artifact_id, parts) -> None:
        part_names = self._resolve_part_names(parts)
        if not self.exists(artifact_id):
            raise KeyError(f"Artifact {artifact_id!r} does not exist.")
        self._delete_many([self._get_part_key(artifact_id, p) for p in part_names])

    def delete_artifact(self, artifact_id) -> None:
        prefix = f"{self._prefix}/{artifact_id}/".lstrip("/")
        keys = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket_id, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        if keys:
            self._delete_many(keys)

    def exists(self, artifact_id) -> bool:
        try:
            self._client.head_object(
                Bucket=self._bucket_id, Key=self._get_root_key(artifact_id)
            )
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def _put(self, key: str, body: bytes, content_type: str = _CT_JSON) -> None:
        size = len(body)

        if size > self._compress_threshold:
            body = gzip.compress(body, compresslevel=6)
            extra_args = {"ContentType": content_type, "ContentEncoding": "gzip"}
        else:
            extra_args = {"ContentType": content_type}

        self._client.upload_fileobj(
            io.BytesIO(body),
            self._bucket_id,
            key,
            ExtraArgs=extra_args,
            Config=self._select_transfer_config(size),
        )

    def _get(self, key: str) -> bytes | None:
        try:
            head = self._client.head_object(Bucket=self._bucket_id, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

        size = head["ContentLength"]
        is_gzip = (
            head.get("ContentEncoding") == "gzip"
            or head.get("Metadata", {}).get("compression") == "gzip"
        )

        buf = io.BytesIO()
        self._client.download_fileobj(
            self._bucket_id,
            key,
            buf,
            Config=self._select_transfer_config(size),
        )
        raw = buf.getvalue()
        return gzip.decompress(raw) if is_gzip else raw

    def _select_transfer_config(self, size: int) -> TransferConfig:
        return (
            self._TRANSFER_LARGE
            if size >= self._multipart_threshold
            else self._TRANSFER_SINGLE
        )

    def _serialize_root(self, artifact: ART) -> bytes:
        return artifact.model_dump_json(exclude=self._part_map.keys()).encode()

    def _get_root_key(self, artifact_id: str) -> str:
        return f"{self._prefix}/{artifact_id}/root.json"

    def _get_part_key(self, artifact_id: str, part_name: str) -> str:
        return f"{self._prefix}/{artifact_id}/parts/{part_name}.bin"

    def _delete_many(self, keys: list[str]) -> None:
        for i in range(0, len(keys), 1000):
            chunk = keys[i : i + 1000]
            self._client.delete_objects(
                Bucket=self._bucket_id,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )

    def close(self):
        self.logger.debug("Closing S3Repo")
        self._pool.shutdown(wait=True)
        self._client.close()
        self.logger.info("S3Repo closed")


def preflight_check(
    s3_client,
    bucket: str,
    write_check: bool,
    logger: logging.Logger,
    prefix: str | None = None,
):
    """
    Runs a staged preflight check against any S3-compatible endpoint.

    Stages:
        1. Connectivity + Auth — get_bucket_location() maps directly to
                                s3:GetBucketLocation, replacing list_buckets()
                                which is not granted by this scoped policy.
        2. Bucket read         — head_bucket() exercises s3:ListBucket.
        3. Write + round-trip  — put → get → delete exercises all three
                                object-level permissions in one pass.
    """
    fail = partial(_fail, logger=logger)

    # Prefix is mandatory: object-level permissions are scoped to prefix/* only.
    if prefix is None:
        fail(
            "A prefix is required. Object-level permissions (GetObject, PutObject, "
            "DeleteObject) are only granted under a prefix path per the bucket policy."
        )

    normalized_prefix = prefix.strip("/") + "/"

    logger.info("Running S3-compatible preflight checks")

    # connectivity + Auth
    logger.info("Checking connectivity and credentials via get_bucket_location...")
    try:
        s3_client.get_bucket_location(Bucket=bucket)
        logger.info("Credentials accepted")
    except NoCredentialsError:
        fail("No credentials configured. Check your access key / secret key.")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch", "403"):
            fail(f"Invalid credentials — {code}")
        elif code in ("404", "NoSuchBucket"):
            fail(f"Bucket '{bucket}' does not exist.")
        else:
            fail(f"Unexpected auth error — {code}: {e}")
    except EndpointResolutionError as e:
        fail(f"Cannot reach endpoint — {e}")
    except Exception as e:
        fail(f"Connection failed — {e}")

    # bucket read (s3:ListBucket)
    logger.info(f"Checking bucket '{bucket}' is accessible...")
    try:
        s3_client.head_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' accessible")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            fail(f"Bucket '{bucket}' does not exist.")
        elif code in ("403", "AccessDenied"):
            fail(f"Bucket '{bucket}' exists but access is denied.")
        else:
            fail(f"Bucket check failed — {code}: {e}")

    #  Object round-trip (s3:PutObject + s3:GetObject + s3:DeleteObject)
    if write_check:
        probe_key = f"{normalized_prefix}.preflight-probe"
        logger.info(
            f"Checking object permissions via round-trip probe '{probe_key}'..."
        )
        try:
            s3_client.put_object(Bucket=bucket, Key=probe_key, Body=b"preflight")
            logger.info("s3:PutObject confirmed")
        except ClientError as e:
            fail(f"PutObject failed — {e.response['Error']['Code']}")

        try:
            s3_client.get_object(Bucket=bucket, Key=probe_key)
            logger.info("s3:GetObject confirmed")
        except ClientError as e:
            # Best-effort cleanup before failing
            with contextlib.suppress(ClientError):
                s3_client.delete_object(Bucket=bucket, Key=probe_key)
            fail(f"GetObject failed — {e.response['Error']['Code']}")

        try:
            s3_client.delete_object(Bucket=bucket, Key=probe_key)
            logger.info("s3:DeleteObject confirmed")
        except ClientError as e:
            fail(f"DeleteObject failed — {e.response['Error']['Code']}")

    logger.info("All preflight checks passed. Starting workflow.")


def _fail(reason: str, logger: logging.Logger):
    logger.error(f"Preflight failed: {reason}")
    raise S3InitError(message="Failed running preflight check.", reason=reason)
