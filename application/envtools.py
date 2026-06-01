from typing import (
    Any,
    Self,
    Mapping, 
    Protocol, 
    TypeAlias, 
    Optional,
    runtime_checkable
)

from pathlib import Path
from dataclasses import dataclass

from prefect.variables import Variable
from dotenv import dotenv_values

from application.utils import is_empty_string, cast

GLOBAL_ENVIRONMENT_FILENAME: str = '.env'

@dataclass(slots=True, frozen=True)
class GlobalEnvironment:
    postgres_user: Optional[str]
    postgres_password: Optional[str]
    postgres_server: Optional[str]
    postgres_port: Optional[int]
    postgres_database_name: Optional[str]

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any], /, allow_empty_values: bool=True) -> Self:
        check_environment_values(data, allow_empty_values=allow_empty_values)
        
        postgres_user           = data['POSTGRES_USER']
        postgres_password       = data['POSTGRES_PASSWORD']
        postgres_server         = data['POSTGRES_SERVER']
        postgres_port           = data['POSTGRES_PORT']
        postgres_database_name  = data['POSTGRES_DATABASE_NAME']

        instance = GlobalEnvironment(
            postgres_user=cast(postgres_user, str, ignore_null=True),
            postgres_password=cast(postgres_password, str, ignore_null=True),
            postgres_server=cast(postgres_server, str, ignore_null=True),
            postgres_port=cast(postgres_port, int, ignore_null=True),
            postgres_database_name=cast(postgres_database_name, str, ignore_null=True)
        )
        return instance

@runtime_checkable
class SupportsGlobalEnvironment(Protocol):
    global_env: GlobalEnvironment


PathLike:   TypeAlias = str | Path


def check_environment_values(data: Mapping[str, Any], /, allow_empty_values: bool=True) -> None:
    if allow_empty_values:
        return 
    for field, value in data.items():
        if value is None or is_empty_string(value):
            raise ValueError(
                f'Missing required environment value for {field!r}'
            )

def read_environment_file(filepath: PathLike, /, allow_empty_values: bool=True) -> dict[str, str | None]:
    environment = dotenv_values(dotenv_path=filepath)
    check_environment_values(environment, allow_empty_values=allow_empty_values)
    return environment



def load_relative_environment_file(anchor: PathLike, *relative_path: PathLike, allow_empty_values: bool=True) -> dict[str, Any]:
    anchor = Path(anchor)
    if not anchor.is_dir():
        anchor = anchor.parent
    filepath = anchor.joinpath(*relative_path)
    return read_environment_file(filepath, allow_empty_values=allow_empty_values)

def load_global_environment_file(allow_empty_values: bool=True) -> GlobalEnvironment:
    values = read_environment_file(GLOBAL_ENVIRONMENT_FILENAME)
    return GlobalEnvironment.from_mapping(values, allow_empty_values=allow_empty_values)

def load_global_environment_prefect(allow_empty_values: bool=True) -> GlobalEnvironment:
    variable_name: str = 'global_environment'

    values = Variable.get(variable_name, None)
    
    if values is None:
        raise ValueError(
            f'The {variable_name!r} variable was not found in Prefect Cloud. '
            f'Make sure it exists and is accessible with the current API credentials.'
        )
    return GlobalEnvironment.from_mapping(values, allow_empty_values=allow_empty_values)

