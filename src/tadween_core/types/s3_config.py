from pydantic import BaseModel, ConfigDict, Field

try:
    from botocore.config import Config
except ImportError:
    raise ImportError("Can't' import boto3. install s3 extension `tadween-core[s3]`")


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
