"""Object storage (S3 / MinIO) helper. boto3 is sync, so calls run in a thread."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import boto3
from botocore.config import Config


class ObjectStore:
    def __init__(
        self, *, endpoint_url: str, access_key: str, secret_key: str, bucket: str, region: str = "us-east-1"
    ) -> None:
        self.bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or None,
            aws_access_key_id=access_key or None,
            aws_secret_access_key=secret_key or None,
            region_name=region,
            config=Config(s3={"addressing_style": "path"}),
        )

    async def ensure_bucket(self) -> None:
        def _do() -> None:
            existing = [b["Name"] for b in self._client.list_buckets().get("Buckets", [])]
            if self.bucket not in existing:
                self._client.create_bucket(Bucket=self.bucket)

        await asyncio.to_thread(_do)

    async def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        await asyncio.to_thread(
            self._client.put_object, Bucket=self.bucket, Key=key, Body=data, ContentType=content_type
        )

    async def get_bytes(self, key: str) -> bytes:
        def _do() -> bytes:
            return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

        return await asyncio.to_thread(_do)

    async def put_json(self, key: str, value: Any) -> None:
        await self.put_bytes(key, json.dumps(value).encode(), "application/json")

    async def get_json(self, key: str) -> Any:
        return json.loads(await self.get_bytes(key))
