from typing import (
    Any, 
    Optional, 
    Protocol, 
    TypeVar, 
    ParamSpec, 
    Callable,
    Self, 
    Concatenate
)
from io import BytesIO

from boto3 import client
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from application.utils import type_name

P = ParamSpec('P')
R = TypeVar('R')


class S3ClientProtocol(Protocol):
    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...
    def head_bucket(self, *, Bucket: str) -> dict[str, Any]: ...
    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...
    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None: ...

class S3ConnectionProtocol(Protocol):
    s3: S3ClientProtocol

def alias(func: Callable[Concatenate[S3ClientProtocol, P], R], /) -> Callable[Concatenate[Self, P], R]:
    def method(self: S3ConnectionProtocol, *args, **kwargs) -> Any:
        return func(self.s3, *args, **kwargs) 
    return method


# NOTE: Public API
def s3_file_exists(s3: S3ClientProtocol, /, bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as error:
        code = error.response['Error']['Code']
        if code in {'404', 'NoSuchKey'}:
            return False
        raise

def s3_bucket_exists(s3: S3ClientProtocol, /, bucket_name: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as error:
        code = error.response['Error']['Code']
        if code in {'404', 'NoSuchKey'}:
            return False
        raise

def s3_read_file_buffer(s3: S3ClientProtocol, /, bucket_name: str, key: str) -> BytesIO:
    response = s3.get_object(Bucket=bucket_name, Key=key)
    body: StreamingBody = response['Body']
    return body.read()

def s3_write_file(s3: S3ClientProtocol, /, bucket_name: str, key: str, content: bytes | BytesIO) -> None:
    if isinstance(content, BytesIO):
        content = content.getvalue()

    if not isinstance(content, bytes):
        raise ValueError(f'content must be bytes or BytesIO, got: {type_name(content)!r}')

    s3.put_object(Bucket=bucket_name, Key=key, Body=content)


class S3Connection(S3ConnectionProtocol):
    def __init__(
            self,
            service_endpoint: str,
            access_key: str,
            access_secret: str,
            region_name: Optional[str]=None
        ) -> None:

        if region_name is None:
            region_name = 'auto'

        self.s3 = client(
            's3',
            endpoint_url=service_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=access_secret,
            region_name=region_name
        )
    
    write_file          = alias(s3_write_file)
    bucket_exists       = alias(s3_bucket_exists)
    file_exists         = alias(s3_file_exists)
    read_file_buffer    = alias(s3_read_file_buffer)













