from __future__ import annotations

from typing import (
    Any,
    TypeGuard,
    Optional, 
    NamedTuple,
    Iterator,
    TypeAlias,
)
from pathlib import Path
from datetime import date

from time import time
from functools import total_ordering
from dataclasses import dataclass

import deltalake as dl
import polars as pl

from application.envtools import (
    SupportsGlobalEnvironment, 
    GlobalEnvironment,
    load_global_environment_file,
    load_relative_environment_file
)
from application.aws3 import S3Connection, build_s3_uri
from application.tables import transform_dataframe

from application.utils import (
    type_name,
    generate_timestamp,
    generate_timehex_token,  
    with_suffix,
    switch
)
from application.loggers import LogFunction, resolve_logger

from data_models import (
    add_hash_columns,
    load_model_compositions_cost_CCD,
    load_model_compositions_cost_CSD,
    load_model_compositions_cost_CSE,
    load_model_materials_services_cost_ICD,
    load_model_materials_services_cost_ISD,
    load_model_materials_services_cost_ISE,
    load_model_cub_m2,
    load_model_cub_m2_composition
)
from flows.extract_construction_data.api_sinapi import is_available_sinapi_data, get_link_to_sinapi_table


SCHEMA_SEPARATOR:           str = '.'
TABLE_NAME_VERSIONS:        str = 'versions'
LOCAL_ENVIRONMENT_FILEPATH: str = '.env.local'

class TableDefinition(NamedTuple):
    name: str

def is_table_definition(value: Any, /) -> TypeGuard[TableDefinition]:
    return isinstance(value, TableDefinition)

class Construction_Tables:
    CUB_M2 = TableDefinition(name='cub_m2')
    CUB_COMPOSITION = TableDefinition(name='cub_composition')
    COMPOSITIONS_CCD = TableDefinition(name='sinapi_compositions_ccd')
    COMPOSITIONS_CSD = TableDefinition(name='sinapi_compositions_csd')
    COMPOSITIONS_CSE = TableDefinition(name='sinapi_compositions_cse')
    MATERIALS_SERVICES_ICD = TableDefinition(name='sinapi_materials_services_icd')
    MATERIALS_SERVICES_ISD = TableDefinition(name='sinapi_materials_services_isd')
    MATERIALS_SERVICES_ISE = TableDefinition(name='sinapi_materials_services_ise')
    
    @classmethod
    def iter_definitions(cls) -> Iterator[TableDefinition]:
        yield from filter(is_table_definition, vars(cls).values())

@total_ordering
@dataclass(slots=True, frozen=True)
class MonthYear:
    month: int
    year: int
    
    @property
    def display(self) -> str:
        return f'{self.month:02d}/{self.year}'
    
    @property 
    def chronological_key(self) -> int: 
        return 100 * self.year + self.month
    
    @property
    def num_months(self) -> int:
        return self.year * 12 + (self.month - 1)

    def __lt__(self, other: MonthYear) -> bool: 
        return self.num_months < other.num_months 
    
    def __eq__(self, other: object) -> bool: 
        if not isinstance(other, MonthYear): 
            return NotImplemented 
        return self.num_months == other.num_months

    def shift(self, months: int, /) -> MonthYear:
        total_months = self.num_months + months
        
        if total_months < 0:
            raise ValueError('Resulting YearMonth is before year 0.')

        year = total_months // 12
        month = (total_months % 12) + 1

        return MonthYear(month=int(month), year=int(year))

@dataclass(slots=True, frozen=True)
class Environment(SupportsGlobalEnvironment):
    schema: str
    bucket_name: str
    ignore_sinapi: bool
    ignore_cub: bool
    global_env: GlobalEnvironment
    
PathLike:           TypeAlias = str | Path
YearValue:          TypeAlias = int
MonthValue:         TypeAlias = int
LiteralMonthYear:   TypeAlias = str
MonthYearLike:      TypeAlias = MonthYear | tuple[MonthValue, YearValue] | LiteralMonthYear


def with_schemas(*schema: str, name: str) -> str:
    return SCHEMA_SEPARATOR.join((*schema, name))

def transform_month_year(data: MonthYearLike, /) -> MonthYear:
    jesus_birthday = 0
    january = 1
    december = 12

    if isinstance(data, tuple):
        month, year = data
        data = MonthYear(month=int(month), year=int(year))

    if isinstance(data, str):
        month, year = data.split('/', maxsplit=1)
        data = MonthYear(month=int(month), year=int(year))

    if not isinstance(data, MonthYear):
        raise ValueError(f'Invalid month input of type {type_name(data)!r}')

    if data.month < january:
        raise ValueError(f'Invalid month {data.month}. Month must be >= 1 (January).')
    
    if data.month > december:
        raise ValueError(f'Invalid month {data.month}. Month must be <= 12 (December).')
    
    if data.year < jesus_birthday:
        raise ValueError(f'Invalid year {data.year}. Year must be >= 0.')

    return data

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
        Construction_Tables.COMPOSITIONS_CCD: load_model_compositions_cost_CCD(year, month),
        Construction_Tables.COMPOSITIONS_CSD: load_model_compositions_cost_CSD(year, month),
        Construction_Tables.COMPOSITIONS_CSE: load_model_compositions_cost_CSE(year, month),
        Construction_Tables.MATERIALS_SERVICES_ICD: load_model_materials_services_cost_ICD(year, month),
        Construction_Tables.MATERIALS_SERVICES_ISD: load_model_materials_services_cost_ISD(year, month),
        Construction_Tables.MATERIALS_SERVICES_ISE: load_model_materials_services_cost_ISE(year, month),
    }
    elapsed_seconds = time() - start
    elapsed_seconds = round(elapsed_seconds, 0)
    count = len(tables)

    logger(f'Extraction took {elapsed_seconds} seconds')
    logger(f'All {count} tables loaded successfully.')

    return tables

def read_all_cub_tables(year: int, month: int, /, logger: Optional[LogFunction]=None) -> dict[TableDefinition, pl.DataFrame]:
    logger = resolve_logger(logger)
    logger(f'Loading CUB/Sinduscon tables for {month:02d}/{year}.')
    
    start = time()
    tables: dict[TableDefinition, pl.DataFrame] = {
        Construction_Tables.CUB_M2: load_model_cub_m2(year, month),
        Construction_Tables.CUB_COMPOSITION: load_model_cub_m2_composition(year, month),
    }
    elapsed_seconds = time() - start
    elapsed_seconds = round(elapsed_seconds, 0)
    count = len(tables)

    logger(f'Extraction took {elapsed_seconds} seconds')
    logger(f'All {count} tables loaded successfully.')

    return tables

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

def resolve_environment(environment: Optional[Environment]=None, /, logger: Optional[LogFunction]=None) -> Environment:
    logger = resolve_logger(logger)
    logger('Resolving environment')

    if environment is None:
        logger('No environment provided, constructing it from local files')
        global_environment = load_global_environment_file(allow_empty_values=True)
        
        values = load_relative_environment_file(__file__, LOCAL_ENVIRONMENT_FILEPATH, allow_empty_values=False)
        
        schema = values['TABLES_SCHEMA']
        bucket_name = values['S3_BUCKET_NAME']
        ignore_cub = values['IGNORE_CUB']
        ignore_sinapi = values['IGNORE_SINAPI']
        
        flags = {
            'TRUE': True, 
            'FALSE': False
        }
        ignore_cub = switch(ignore_cub, flags)
        ignore_sinapi = switch(ignore_sinapi, flags)

        environment = Environment(
            schema=str(schema), 
            bucket_name=str(bucket_name),
            ignore_sinapi=bool(ignore_sinapi),
            ignore_cub=bool(ignore_cub),
            global_env=global_environment)
        
    if not isinstance(environment, Environment):
        raise ValueError('environment must be an instance of Environment')
    
    if environment.schema is None:
        raise ValueError('environment schema must not be None')
    
    if not isinstance(environment.ignore_sinapi, bool):
        raise ValueError(f'environment ignore_sinapi must be boolean, but got: {type_name(environment.ignore_sinapi)!r}')
    
    if not isinstance(environment.ignore_cub, bool):
        raise ValueError(f'environment ignore_cub must be boolean, but got: {type_name(environment.ignore_cub)!r}')
    
    if any({
        environment.global_env.s3_access_key is None,
        environment.global_env.s3_access_secret is None,
        environment.global_env.s3_service_endpoint is None,

    }):
        raise ValueError('Incomplete S3 configuration in environment')    
    
    logger('Environment successfully resolved')
    return environment

def resolve_s3_connection(environment: Environment, logger: Optional[LogFunction]=None) -> S3Connection:
    logger = resolve_logger(logger)
    logger('Start resolving S3 connection')

    connection = S3Connection(
        service_endpoint=environment.global_env.s3_service_endpoint,
        access_key=environment.global_env.s3_access_key,
        secret_key=environment.global_env.s3_access_secret
    )
    logger('S3 connection created')
    return connection

def resolve_latest(s3_connection: S3Connection, environment: Environment, /, ignore_missing_tables: bool=False, logger: Optional[LogFunction]=None) -> None:
    logger = resolve_logger(logger)
    logger('Starting latest table resolution process')

    for definition in Construction_Tables.iter_definitions():
        table_name = with_schemas(environment.schema, name=definition.name)
        table_name_latest = with_suffix(table_name, '_latest')
    
        s3_link = build_s3_uri(environment.bucket_name, table_name)
        s3_link_latest = build_s3_uri(environment.bucket_name, table_name_latest)

        try:
            s3_connection.assert_resource_exists(environment.bucket_name, table_name)
        except FileExistsError:
            if not ignore_missing_tables:
                raise
            logger(f'{table_name} not exists. Skipping...')
            continue
            
        logger(f'Pushing latest from {table_name!r} to {table_name_latest!r}. Link: {s3_link_latest!r}')

        dataframe = pl.scan_delta(s3_link, storage_options=s3_connection.credentials)
        dataframe = dataframe.with_columns(
            pl.col('CREATED_AT').cast(pl.Datetime)
        )
        versions = dataframe.group_by('YEAR_MONTH').agg(
            pl.col('CREATED_AT').max()
        )
        dataframe = dataframe.join(versions, on=['YEAR_MONTH', 'CREATED_AT'], how='inner')
        dataframe.sink_csv(s3_link_latest, compression='gzip', check_extension=False, storage_options=s3_connection.credentials)   

    logger('Latest tables updated successfully')

def extract_construction_data(
        start: Optional[MonthYearLike]=None,
        finish: Optional[MonthYearLike]=None,
        environment: Optional[Environment]=None,
        update_latest_only: bool=False,
        logger: Optional[LogFunction]=None,  
    ) -> None:

    logger = resolve_logger(logger)
    environment = resolve_environment(environment, logger=logger)
    s3_connection = resolve_s3_connection(environment, logger=logger)

    s3_connection.assert_bucket_exists(environment.bucket_name)
    
    ignore_missing_tables = environment.ignore_sinapi or environment.ignore_cub

    if update_latest_only:
        logger(f'Updating latest tables only. Ignoring data extraction.')
        resolve_latest(s3_connection, environment, ignore_missing_tables=ignore_missing_tables, logger=logger)
        return
    
    periods = resolve_periods(start, finish, logger=logger)
    
    # NOTE: Generating UTC-timestamp and token for entire process
    token = generate_timehex_token(4)
    timestamp = generate_timestamp()

    total = len(periods)

    for index, period in enumerate(periods, start=1):
        logger(f'{index}/{total} - Processing period {period.display}.')
        
        tables: dict[TableDefinition, pl.DataFrame] = {}

        if not environment.ignore_sinapi:
            tables.update(read_all_sinapi_tables(period.year, period.month, logger=logger))
        
        if not environment.ignore_cub:
            tables.update(read_all_cub_tables(period.year, period.month, logger=logger))

        # NOTE: Here timestamp and ID are overwrited
        for definition, dataframe in tables.items():
            table_name = with_schemas(environment.schema, name=definition.name)
            s3_link = build_s3_uri(environment.bucket_name, table_name)
            dataframe = add_hash_columns(dataframe, token=token, timestamp=timestamp)

            logger(f'Inserting {dataframe.height} new rows to DeltaLake table {table_name!r}. Link: {s3_link!r}')    
            dl.write_deltalake(
                s3_link,
                data=dataframe.to_arrow(),
                storage_options=s3_connection.credentials,
                schema_mode=None,
                partition_by=['YEAR_MONTH'],
                mode='append'
            )
    resolve_latest(s3_connection, environment, ignore_missing_tables=ignore_missing_tables, logger=logger)