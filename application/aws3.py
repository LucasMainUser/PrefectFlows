from typing import (
    Any, 
    Optional, 
    Protocol, 
    TypeVar, 
    ParamSpec, 
    Callable,
    Self, 
    Concatenate,
    Literal,
    overload
)
from io import BytesIO

from boto3 import client
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.paginate import Paginator

from application.utils import type_name, cache

P = ParamSpec('P')
R = TypeVar('R')


class S3ClientProtocol(Protocol):
    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...
    def head_bucket(self, *, Bucket: str) -> dict[str, Any]: ...
    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...
    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None: ...
    def get_paginator(self, operation_name: str) -> Paginator: ...
    def list_objects_v2(self, *, Bucket: str, Prefix: Optional[str] = ..., MaxKeys: Optional[int] = ..., ContinuationToken: Optional[str] = ...) -> dict[str, Any]: ...

class S3ConnectionProtocol(Protocol):
    s3: S3ClientProtocol

def alias(func: Callable[Concatenate[S3ClientProtocol, P], R], /) -> Callable[Concatenate[Self, P], R]:
    def method(self: S3ConnectionProtocol, *args, **kwargs) -> Any:
        return func(self.s3, *args, **kwargs) 
    return method


# NOTE: Public API
def s3_bucket_exists(s3: S3ClientProtocol, /, bucket_name: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as error:
        code = error.response['Error']['Code']
        if code in {'404', 'NoSuchKey'}:
            return False
        raise

def s3_folder_exists(s3: S3ClientProtocol, /, bucket_name: str, prefix: str) -> bool:
    prefix = str(prefix).removesuffix('/') + '/'
    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=1,
        )
        return 'Contents' in response
    except ClientError as error:
        code = error.response['Error']['Code']
        if code in {'404', 'NoSuchKey', 'NoSuchBucket'}:
            return False
        raise

def s3_file_exists(s3: S3ClientProtocol, /, bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as error:
        code = error.response['Error']['Code']
        if code in {'404', 'NoSuchKey'}:
            return False
        raise
    
def s3_resource_exists(s3: S3ClientProtocol, /, bucket_name: str, key_or_prefix: str) -> bool:
    return s3_file_exists(s3, bucket_name, key_or_prefix) or s3_folder_exists(s3, bucket_name, key_or_prefix)

def s3_assert_bucket_exists(s3: S3ClientProtocol, /, bucket_name: str) -> None:
    if not s3_bucket_exists(s3, bucket_name=bucket_name):
        raise FileExistsError(f'S3 bucket {bucket_name!r} does not exist or is not accessible')

def s3_assert_file_exists(s3: S3ClientProtocol, /, bucket_name: str, key: str) -> None:
    if not s3_file_exists(s3, bucket_name=bucket_name, key=key):
        raise FileExistsError(f'S3 file {key!r} in {bucket_name!r} does not exist or is not accessible')

def s3_assert_folder_exists(s3: S3ClientProtocol, /, bucket_name: str, prefix: str) -> None:
    if not s3_folder_exists(s3, bucket_name=bucket_name, prefix=prefix):
        raise FileExistsError(f'S3 folder {prefix!r} in {bucket_name!r} does not exist or is not accessible')

def s3_assert_resource_exists(s3: S3ClientProtocol, /, bucket_name: str, key_or_prefix: str) -> None:
    if not s3_resource_exists(s3, bucket_name=bucket_name, key_or_prefix=key_or_prefix):
        raise FileExistsError(f'S3 resource (file or folder) {key_or_prefix!r} in {bucket_name!r} does not exist or is not accessible')
    
@overload
def s3_read_file(s3: S3ClientProtocol, /, bucket_name: str, key: str, buffer: Literal[False]=False) -> bytes: ...

@overload
def s3_read_file(s3: S3ClientProtocol, /, bucket_name: str, key: str, buffer: Literal[True]=True) -> BytesIO: ...

def s3_read_file(s3: S3ClientProtocol, /, bucket_name: str, key: str, buffer: bool=False) -> BytesIO | bytes:
    response = s3.get_object(Bucket=bucket_name, Key=key)
    body: StreamingBody = response['Body']

    if not buffer:
        return body.read()
    
    bytes_buffer = BytesIO(body.read())
    bytes_buffer.seek(0)

    return bytes_buffer


def s3_write_file(s3: S3ClientProtocol, /, bucket_name: str, key: str, content: bytes | BytesIO) -> None:
    if isinstance(content, BytesIO):
        content = content.getvalue()

    if not isinstance(content, bytes):
        raise ValueError(f'content must be bytes or BytesIO, got: {type_name(content)!r}')

    s3.put_object(Bucket=bucket_name, Key=key, Body=content)

def s3_list_filenames(s3: S3ClientProtocol, /, bucket_name: str) -> list[str]:
    filenames: list[str] = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket_name):
        page: dict
        for obj in page.get('Contents', []):
            filename = obj['Key']
            filename = str(filename)
            filenames.append(filename)
    return filenames

@cache
def build_s3_uri(bucket_name: str, key: str, /) -> str:
    key = key.lstrip('/')
    return f's3://{bucket_name}/{key}'

@cache
def build_s3_credentials(
        *,
        access_key: str,
        secret_key: str,
        region: Optional[str] = None,
        service_endpoint: Optional[str] = None,
    ) -> dict[str, str]:

    options: dict[str, str] = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
    }

    if region:
        options['aws_region'] = region

    if service_endpoint:
        options['endpoint_url'] = service_endpoint

    return options

class S3Connection(S3ConnectionProtocol):
    def __init__(
            self,
            service_endpoint: str,
            access_key: str,
            secret_key: str,
            region: Optional[str]=None
        ) -> None:

        if region is None:
            region = 'auto'

        self.service_endpoint = str(service_endpoint)
        self.access_key = str(access_key)
        self.secret_key = str(secret_key)
        self.region = str(region)
        
        self.s3 = client(
            's3',
            endpoint_url=self.service_endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
        )
    
    @property
    def credentials(self) -> dict[str, str]:
        return build_s3_credentials(
            access_key=self.access_key,
            secret_key=self.secret_key,
            region=self.region,
            service_endpoint=self.service_endpoint
        )
    
    file_exists             = alias(s3_file_exists)
    bucket_exists           = alias(s3_bucket_exists)
    folder_exists           = alias(s3_folder_exists)
    resource_exists         = alias(s3_resource_exists)
    assert_file_exists      = alias(s3_assert_file_exists)
    assert_bucket_exists    = alias(s3_assert_bucket_exists)
    assert_folder_exists    = alias(s3_assert_folder_exists)
    assert_resource_exists  = alias(s3_assert_resource_exists)
    
    read_file               = alias(s3_read_file)
    write_file              = alias(s3_write_file)
    list_filenames          = alias(s3_list_filenames)











