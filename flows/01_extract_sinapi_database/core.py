from __future__ import annotations

from typing import (
    Optional, 
    NamedTuple,
    Mapping, 
    TypeAlias
)
from pathlib import Path
from datetime import date
from dataclasses import dataclass

import psycopg as ps
import psycopg.sql as sql

import polars as pl

from application.envtools import (
    SupportsGlobalEnvironment, 
    GlobalEnvironment,
    load_global_environment_file,
    load_relative_environment_file
)
from application.database import postgres_connection_string, create_schema
from application.tables import PolarsLike, transform_dataframe
from application.utils import (
    cache,
    type_name,
    generate_timestamp,
    generate_timehex_token,  
)
from application.loggers import LogFunction, resolve_logger

from data_models import (
    add_hash_columns,
    load_compositions_cost_CCD,
    load_compositions_cost_CSD,
    load_compositions_cost_CSE,
    load_materials_services_cost_ICD,
    load_materials_services_cost_ISD,
    load_materials_services_cost_ISE
)
from sinapi_api import is_available_sinapi_data, get_link_to_sinapi_table


LOCAL_ENVIRONMENT_FILEPATH: str = '.env.local'

@cache
def sinapi_sql_schema() -> str:
    schema = '''
    "CREATED_AT"    TIMESTAMP WITH TIME ZONE NOT NULL,
    "ID"            TEXT NOT NULL,
    "GROUP"         TEXT,
    "CODE"          TEXT,
    "DESCRIPTION"   TEXT,
    "UNIT"          TEXT,
    "PRICING"       TEXT,
    "UF"            TEXT,
    "STATE"         TEXT,
    "CITY"          TEXT,
    "VALUE_TYPE"    TEXT,
    "VALUE"         DOUBLE PRECISION,
    "YEAR"          INT NOT NULL,
    "MONTH"         INT NOT NULL,
    "YEAR_MONTH"    INT NOT NULL
    '''
    return schema.strip()

class SQLTableDefinition(NamedTuple):
    name: str
    schema: str

# NOTE: Sync these names in the views-query file
class SINAPI_Tables:
    COMPOSITIONS_CCD = SQLTableDefinition(name='compositions_ccd', schema=sinapi_sql_schema())
    COMPOSITIONS_CSD = SQLTableDefinition(name='compositions_csd', schema=sinapi_sql_schema())
    COMPOSITIONS_CSE = SQLTableDefinition(name='compositions_cse', schema=sinapi_sql_schema())
    MATERIALS_SERVICES_ICD = SQLTableDefinition(name='materials_services_icd', schema=sinapi_sql_schema())
    MATERIALS_SERVICES_ISD = SQLTableDefinition(name='materials_services_isd', schema=sinapi_sql_schema())
    MATERIALS_SERVICES_ISE = SQLTableDefinition(name='materials_services_ise', schema=sinapi_sql_schema())



class YearMonth(NamedTuple):
    year: int
    month: int
    
    @property
    def display(self) -> str:
        return f'{self.month:02d}/{self.year}'
    
    def shift(self, months: int, /) -> YearMonth:
        total_months = (self.year * 12 + (self.month - 1)) + months
        
        if total_months < 0:
            raise ValueError('Resulting YearMonth is before year 0.')

        year = total_months // 12
        month = (total_months % 12) + 1

        return YearMonth(year, month)

@dataclass(slots=True, frozen=True)
class Environment(SupportsGlobalEnvironment):
    schema: str
    global_env: GlobalEnvironment
    
PathLike:           TypeAlias = str | Path
Year:               TypeAlias = int
Month:              TypeAlias = int
LiteralMonthYear:   TypeAlias = str
YearMonthLike:      TypeAlias = YearMonth | tuple[Year, Month] | LiteralMonthYear




def transform_year_month(data: YearMonthLike, /) -> YearMonth:
    jesus_birthday = 0
    january = 1
    december = 12

    if isinstance(data, tuple):
        data = YearMonth(*data)
    
    if isinstance(data, str):
        month, year = data.split('/', maxsplit=1)
        data = YearMonth(year, month)

    if not isinstance(data, YearMonth):
        raise ValueError(f'Invalid year-month input of type {type_name(data)!r}')

    if data.month < january:
        raise ValueError(f'Invalid month {data.month}. Month must be >= 1 (January).')
    
    if data.month > december:
        raise ValueError(f'Invalid month {data.month}. Month must be <= 12 (December).')
    
    if data.year < jesus_birthday:
        raise ValueError(f'Invalid year {data.year}. Year must be >= 0.')

    return data

def database_insert_tables(
        tables: Mapping[SQLTableDefinition, PolarsLike], 
        connection: ps.Connection,
        schema: Optional[str]=None,
        logger: Optional[LogFunction]=None
    ) -> None:
    
    logger = resolve_logger(logger)
    logger('Starting writing tables, waiting...')
    
    with connection.transaction():
        if schema is not None:
            create_schema(schema, connection, commit=False)

        for table_definition, data in tables.items():
            name = str(table_definition.name)
            table_sql_schema = sql.SQL(table_definition.schema)
            
            dataframe = transform_dataframe(data)
            
            table_name = sql.Identifier(name)

            if schema is not None:
                table_name = sql.Identifier(schema, name)
        
            column_names = sql.SQL(', ').join(
                sql.Identifier(column_name) for column_name in dataframe.columns)

            copy_query      = sql.SQL('COPY {} ({}) FROM STDIN WITH (FORMAT csv)').format(table_name, column_names)
            creation_query  = sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(table_name, table_sql_schema)    
            
            connection.execute(creation_query)
            
            with connection.cursor().copy(copy_query) as copy:
                for chunk in dataframe.iter_slices(50_000):
                    copy.write(
                        chunk.write_csv(include_header=False).encode('utf-8')
                    )
            logger(f'Written {name!s} successfully ({dataframe.height} rows)') 

    logger(f'All tables append successfully, commiting.') 
    

def resolve_sinapi_tables(year: int, month: int, /, logger: Optional[LogFunction]=None) -> dict[SQLTableDefinition, pl.DataFrame]:
    logger = resolve_logger(logger)
    logger(f'Loading SINAPI tables for {month:02d}/{year}.')

    if not is_available_sinapi_data(year, month):
        raise ValueError(f'SINAPI data is not available for {month:02d}/{year}.')

    sinapi_link = get_link_to_sinapi_table(year, month)
    logger(f'SINAPI source link: {sinapi_link}')

    logger(f'Extracting for {month:02d}/{year}, this may take a while ...')
    tables: dict[SQLTableDefinition, pl.DataFrame] = {
        SINAPI_Tables.COMPOSITIONS_CCD: load_compositions_cost_CCD(year, month),
        SINAPI_Tables.COMPOSITIONS_CSD: load_compositions_cost_CSD(year, month),
        SINAPI_Tables.COMPOSITIONS_CSE: load_compositions_cost_CSE(year, month),
        SINAPI_Tables.MATERIALS_SERVICES_ICD: load_materials_services_cost_ICD(year, month),
        SINAPI_Tables.MATERIALS_SERVICES_ISD: load_materials_services_cost_ISD(year, month),
        SINAPI_Tables.MATERIALS_SERVICES_ISE: load_materials_services_cost_ISE(year, month),
    }
    count = len(tables)

    logger(f'All {count} tables loaded successfully.')
    return tables

def resolve_periods(start: YearMonthLike | None, finish: YearMonthLike | None, /, logger: Optional[LogFunction]=None) -> list[YearMonth]:
    logger = resolve_logger(logger)
    
    if (start is None) or (finish is None):
        today = date.today()
        current_month = YearMonth(year=today.year, month=today.month)
        previous_month = current_month.shift(-1)

        logger(f'Start or finish month missing. Defaulting to previous month ({previous_month.display})')
        return [previous_month]
    
    logger('Resolving period range')

    start  = transform_year_month(start)
    finish = transform_year_month(finish)
    
    if start > finish:
        raise ValueError(
            f'Invalid range: start ({start.display}) must be less than or equal to finish ({finish.display}).'
        )
    
    output: list[YearMonth] = []
    current = start

    while current <= finish:
        output.append(current)
        current = current.shift(1)
    count = len(output)

    logger(f'Resolved {count} periods from {start.display} to {finish.display}')
    return output

def resolve_environment(environment: Optional[Environment]=None, /, logger: Optional[LogFunction]=None) -> Environment:
    logger = resolve_logger(logger)
    logger('Resolving environment')

    if environment is None:
        logger('No environment provided, constructing it from local files')
        global_environment = load_global_environment_file(allow_empty_values=True)
        
        values = load_relative_environment_file(__file__, LOCAL_ENVIRONMENT_FILEPATH, allow_empty_values=False)
        schema = values['SCHEMA']

        environment = Environment(
            schema=schema, global_env=global_environment)

    if not isinstance(environment, Environment):
        raise ValueError('environment must be an instance of Environment')
    
    if environment.schema is None:
        raise ValueError('environment schema must not be None')

    if any({
        environment.global_env.postgres_user is None,
        environment.global_env.postgres_password is None,
        environment.global_env.postgres_port is None,
        environment.global_env.postgres_server is None,
        environment.global_env.postgres_database_name is None,

    }):
        raise ValueError('Incomplete PostgreSQL configuration in environment')    
    
    logger('Environment successfully resolved')
    return environment

def resolve_database_connection_string(environment: Environment, /, logger: Optional[LogFunction]=None) -> str:
    logger = resolve_logger(logger)
    logger('Writing database connection string')
    connection_string = postgres_connection_string(
        user=environment.global_env.postgres_user,
        password=environment.global_env.postgres_password,
        port=environment.global_env.postgres_port,
        server=environment.global_env.postgres_server,
        database_name=environment.global_env.postgres_database_name
    )
    logger('Database connection string done!')
    return connection_string

def extract_sinapi_data_to_postgres(
        start: Optional[YearMonthLike]=None,
        finish: Optional[YearMonthLike]=None,
        environment: Optional[Environment]=None,
        logger: Optional[LogFunction]=None    
    ) -> None:

    logger = resolve_logger(logger)
    periods = resolve_periods(start, finish, logger=logger)
    environment = resolve_environment(environment, logger=logger)
    connection_string = resolve_database_connection_string(environment, logger=logger)

    # NOTE: Generate timestamp and ID ("hash") for entire process
    token = generate_timehex_token(4)
    timestamp = generate_timestamp()
    
    total = len(periods)
    with ps.connect(connection_string) as connection:
        for index, period in enumerate(periods, start=1):
            logger(f'{index}/{total} - Processing period {period.display}.')
            
            tables = resolve_sinapi_tables(period.year, period.month, logger=logger)

            # NOTE: Here the timestamp and ID are overwrited
            for key, dataframe in tables.items():
                tables[key] = add_hash_columns(dataframe, token=token, timestamp=timestamp)
            
            database_insert_tables(tables, connection, schema=environment.schema, logger=logger)            
