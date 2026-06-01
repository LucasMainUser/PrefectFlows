from typing import Optional

from dataclasses import dataclass
from importlib.util import find_spec

from psycopg import Connection
from psycopg.sql import SQL, Identifier

@dataclass(slots=True, frozen=True)
class DatabaseConnectionConfig:
    dialect: str
    driver: Optional[str]=None
    user: Optional[str]=None
    password: Optional[str]=None
    server: Optional[str]=None
    port: Optional[str]=None
    database_name: Optional[str]=None


def check_driver(driver: str, /) -> None:
    if find_spec(driver) is None:
        raise ImportError(f'Driver {driver!r} is not installed.')

def create_schema(schema: str, connection: Connection, /, commit: bool=True) -> None:
    safe_query = SQL('CREATE SCHEMA IF NOT EXISTS {}').format(Identifier(schema))
    with connection.cursor() as cursor:
        cursor.execute(safe_query)
    if commit:
        connection.commit()


def postgres_connection_string(
        user: str,
        password: str,
        server: str,
        port: str,
        database_name: str
    ) -> str:
    return f'postgresql://{user}:{password}@{server}:{port}/{database_name}'





