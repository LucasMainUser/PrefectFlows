from __future__ import annotations

from typing import (
    Any,
    TypeGuard,
    Optional, 
    NamedTuple,
    Mapping, 
    Iterator,
    TypeAlias
)
from io import BytesIO
from pathlib import Path
from datetime import date

from time import time
from dataclasses import dataclass

import polars as pl
import prefect as pf

from application.envtools import (
    SupportsGlobalEnvironment, 
    GlobalEnvironment,
    load_global_environment_file,
    load_relative_environment_file
)
from application.aws3 import S3Connection
from application.tables import PolarsLike, transform_dataframe
from application.utils import (
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


class TableDefinition(NamedTuple):
    name: str

def is_table_definition(value: Any, /) -> TypeGuard[TableDefinition]:
    return isinstance(value, TableDefinition)

# NOTE: Sync these names in the views-query file
class SINAPI_Tables:
    COMPOSITIONS_CCD = TableDefinition(name='compositions_ccd')
    COMPOSITIONS_CSD = TableDefinition(name='compositions_csd')
    COMPOSITIONS_CSE = TableDefinition(name='compositions_cse')
    MATERIALS_SERVICES_ICD = TableDefinition(name='materials_services_icd')
    MATERIALS_SERVICES_ISD = TableDefinition(name='materials_services_isd')
    MATERIALS_SERVICES_ISE = TableDefinition(name='materials_services_ise')


    @classmethod
    def iter_definitions(cls) -> Iterator[TableDefinition]:
        yield from filter(is_table_definition, vars(cls).values())


class MonthYear(NamedTuple):
    month: int
    year: int
    
    @property
    def display(self) -> str:
        return f'{self.month:02d}/{self.year}'
    
    def shift(self, months: int, /) -> MonthYear:
        total_months = (self.year * 12 + (self.month - 1)) + months
        
        if total_months < 0:
            raise ValueError('Resulting YearMonth is before year 0.')

        year = total_months // 12
        month = (total_months % 12) + 1

        return MonthYear(month=month, year=year)

@dataclass(slots=True, frozen=True)
class Environment(SupportsGlobalEnvironment):
    schema: str
    bucket_name: str
    global_env: GlobalEnvironment
    
PathLike:           TypeAlias = str | Path
Year:               TypeAlias = int
Month:              TypeAlias = int
LiteralMonthYear:   TypeAlias = str
MonthYearLike:      TypeAlias = MonthYear | tuple[Month, Year] | LiteralMonthYear


def with_schema(schema: str, name: str, /) -> str:
    return f'{schema}.{name}'

def transform_month_year(data: MonthYearLike, /) -> MonthYear:
    jesus_birthday = 0
    january = 1
    december = 12

    if isinstance(data, tuple):
        data = MonthYear(*map(int, data))
    
    if isinstance(data, str):
        month, year = data.split('/', maxsplit=1)
        data = MonthYear(month=int(month), year=int(year))

    if not isinstance(data, MonthYear):
        raise ValueError(f'Invalid month-year input of type {type_name(data)!r}')

    if data.month < january:
        raise ValueError(f'Invalid month {data.month}. Month must be >= 1 (January).')
    
    if data.month > december:
        raise ValueError(f'Invalid month {data.month}. Month must be <= 12 (December).')
    
    if data.year < jesus_birthday:
        raise ValueError(f'Invalid year {data.year}. Year must be >= 0.')

    return data

@pf.task
def read_all_sinapi_tables(year: int, month: int, /, logger: Optional[LogFunction]=None) -> dict[TableDefinition, pl.DataFrame]:
    logger = resolve_logger(logger)
    logger(f'Loading SINAPI tables for {month:02d}/{year}.')

    if not is_available_sinapi_data(year, month):
        raise ValueError(f'SINAPI data is not available for {month:02d}/{year}.')

    sinapi_link = get_link_to_sinapi_table(year, month)
    logger(f'SINAPI source link: {sinapi_link}')

    logger(f'Extracting for {month:02d}/{year}, this may take a while ...')

    start = time()
    tables: dict[TableDefinition, pl.DataFrame] = {
        SINAPI_Tables.COMPOSITIONS_CCD: load_compositions_cost_CCD(year, month),
        SINAPI_Tables.COMPOSITIONS_CSD: load_compositions_cost_CSD(year, month),
        SINAPI_Tables.COMPOSITIONS_CSE: load_compositions_cost_CSE(year, month),
        SINAPI_Tables.MATERIALS_SERVICES_ICD: load_materials_services_cost_ICD(year, month),
        SINAPI_Tables.MATERIALS_SERVICES_ISD: load_materials_services_cost_ISD(year, month),
        SINAPI_Tables.MATERIALS_SERVICES_ISE: load_materials_services_cost_ISE(year, month),
    }
    elapsed_seconds = time() - start

    logger(f'Extraction took {elapsed_seconds} seconds')

    count = len(tables)
    logger(f'All {count} tables loaded successfully.')

    return tables

@pf.task
def fetch_sinapi_database(s3_connection: S3Connection, environment: Environment, /, logger: Optional[LogFunction]=None) -> dict[TableDefinition, pl.DataFrame]:
    logger = resolve_logger(logger)
    logger('Reading database from CloudFlare (S3-Service) parquet-files')

    database: dict[TableDefinition, pl.DataFrame] = {}

    for definition in SINAPI_Tables.iter_definitions():
        dataframe = pl.DataFrame()
        table_name = with_schema(environment.schema, definition.name)

        logger(f'Loading - {table_name!r}')

        if s3_connection.file_exists(environment.bucket_name, table_name):      
            parquet_buffer = s3_connection.read_file_buffer(environment.bucket_name, table_name)
            dataframe = pl.read_parquet(parquet_buffer)
        
        database[definition] = dataframe

    logger('CloudFlare database ready!')
    return database

@pf.task
def insert_data(table: PolarsLike, definition: TableDefinition, database: Mapping[TableDefinition, PolarsLike], /) -> dict[TableDefinition, pl.DataFrame]:
    database = {
        key: transform_dataframe(data) for key, data in database.items() 
    }
    if definition not in database:
        database[definition] = transform_dataframe(table)
        return database
    
    database[definition] = pl.concat((
        database[definition], transform_dataframe(table)
    ))
    return database

@pf.task
def mount_latests(tables: Mapping[TableDefinition, PolarsLike], /, logger: Optional[LogFunction]=None) -> dict[TableDefinition, pl.DataFrame]:
    logger = resolve_logger(logger)
    logger('Start mounting latests')

    latests: dict[TableDefinition, pl.DataFrame] = {}

    for definition, data in tables.items():
        latests_table_name = f'{definition.name}_latests'
        logger(f'Extracting latests records from {definition.name!r} to {latests_table_name!r}')

        latest_definition = TableDefinition(name=latests_table_name)

        dataframe = transform_dataframe(data)
        dataframe = dataframe.with_columns(
            pl.col('CREATED_AT').max().over('YEAR_MONTH').alias('LATEST_CREATED')
        )
        dataframe = dataframe.filter(
            pl.col('CREATED_AT') == pl.col('LATEST_CREATED')
        )
        dataframe = dataframe.drop('LATEST_CREATED')

        latests[latest_definition] = dataframe

    logger('Finished mounting latest tables')
    return latests

@pf.task
def push_sinapi_database(s3_connection: S3Connection, environment: Environment, tables: Mapping[TableDefinition, PolarsLike], /, logger: Optional[LogFunction]=None) -> None:
    logger = resolve_logger(logger)
    logger('Start pushing sinapi database')

    for definition, data in tables.items():
        table_name = with_schema(environment.schema, definition.name)
        buffer = BytesIO()

        logger(f'Writing {table_name!r}')

        transform_dataframe(data).write_parquet(buffer)
        buffer.seek(0)

        s3_connection.write_file(
            bucket_name=environment.bucket_name, key=table_name, content=buffer)

    logger('Finished pushing sinapi database')


@pf.task
def resolve_periods(start: MonthYearLike | None, finish: MonthYearLike | None, /, logger: Optional[LogFunction]=None) -> list[MonthYear]:
    logger = resolve_logger(logger)
    
    if (start is None) or (finish is None):
        today = date.today()
        current_month = MonthYear(month=today.month, year=today.year)
        previous_month = current_month.shift(-1)

        logger(f'Start or finish month missing. Defaulting to previous month ({previous_month.display})')
        return [previous_month]
    
    logger('Resolving period range')

    start  = transform_month_year(start)
    finish = transform_month_year(finish)
    
    if start > finish:
        raise ValueError(
            f'Invalid range: start ({start.display}) must be less than or equal to finish ({finish.display}).'
        )
    
    output: list[MonthYear] = []
    current = start

    while current <= finish:
        output.append(current)
        current = current.shift(1)
    count = len(output)

    logger(f'Resolved {count} periods from {start.display} to {finish.display}')
    return output

@pf.task
def resolve_environment(environment: Optional[Environment]=None, /, logger: Optional[LogFunction]=None) -> Environment:
    logger = resolve_logger(logger)
    logger('Resolving environment')

    if environment is None:
        logger('No environment provided, constructing it from local files')
        global_environment = load_global_environment_file(allow_empty_values=True)
        
        values = load_relative_environment_file(__file__, LOCAL_ENVIRONMENT_FILEPATH, allow_empty_values=False)
        
        schema = values['SCHEMA']
        bucket_name = values['S3_BUCKET_NAME']

        environment = Environment(
            schema=str(schema), 
            bucket_name=str(bucket_name), 
            global_env=global_environment)
        
    if not isinstance(environment, Environment):
        raise ValueError('environment must be an instance of Environment')
    
    if environment.schema is None:
        raise ValueError('environment schema must not be None')

    if any({
        environment.global_env.s3_access_key is None,
        environment.global_env.s3_access_secret is None,
        environment.global_env.s3_service_endpoint is None,

    }):
        raise ValueError('Incomplete S3 configuration in environment')    
    
    logger('Environment successfully resolved')
    return environment

@pf.task
def resolve_s3_connection(environment: Environment, logger: Optional[LogFunction]=None) -> S3Connection:
    logger = resolve_logger(logger)
    logger('Start resolving S3 connection')

    connection = S3Connection(
        service_endpoint=environment.global_env.s3_service_endpoint,
        access_key=environment.global_env.s3_access_key,
        access_secret=environment.global_env.s3_access_secret
    )
    logger('S3 connection created')
    return connection

def extract_sinapi_data(
        start: Optional[MonthYearLike]=None,
        finish: Optional[MonthYearLike]=None,
        environment: Optional[Environment]=None,
        logger: Optional[LogFunction]=None    
    ) -> None:

    logger = resolve_logger(logger)
    periods = resolve_periods(start, finish, logger=logger)
    environment = resolve_environment(environment, logger=logger)
    s3_connection = resolve_s3_connection(environment, logger=logger)

    if not s3_connection.bucket_exists(environment.bucket_name):
        raise RuntimeError(f'S3 bucket {environment.bucket_name!r} does not exist or is not accessible')

    database = fetch_sinapi_database(s3_connection, environment, logger=logger)
    
    # NOTE: Generate timestamp and ID ('hash') for entire process
    token = generate_timehex_token(4)
    timestamp = generate_timestamp()
    
    total = len(periods)

    for index, period in enumerate(periods, start=1):
        logger(f'{index}/{total} - Processing period {period.display}.')
        
        tables = read_all_sinapi_tables(period.year, period.month, logger=logger)

        # NOTE: Here timestamp and ID are overwrited
        for key, dataframe in tables.items():
            dataframe = add_hash_columns(dataframe, token=token, timestamp=timestamp)
            database = insert_data(dataframe, key, database)

    latests = mount_latests(database, logger=logger)
    push_sinapi_database(s3_connection, environment, {**database, **latests}, logger=logger)
