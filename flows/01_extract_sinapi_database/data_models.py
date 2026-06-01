from typing import Optional

import polars as pl

from application.utils import fill_whitespaces, generate_timehex_token, generate_timestamp
from application.tables import (
    PolarsLike,
    transform_dataframe,
    skip_rows, 
    first_row_as_header, 
    transform_headers,
    any_of,
    replace_whitespaces,
    forward_fill,
    lowercase_col,
    sanitize_text,
    select_casting,
    map_elements
)
from sinapi_api import load_sinapi_dataframe

MAP_STATE_NAME = {
    'AC': 'RIO BRANCO',
    'AL': 'MACEIO',
    'AM': 'MANAUS',
    'AP': 'MACAPA',
    'BA': 'SALVADOR',
    'CE': 'FORTALEZA',
    'DF': 'BRASILIA',
    'ES': 'VITORIA',
    'GO': 'GOIANIA',
    'MA': 'SAO LUIS',
    'MG': 'BELO HORIZONTE',
    'MS': 'CAMPO GRANDE',
    'MT': 'CUIABA',
    'PA': 'BELEM',
    'PB': 'JOAO PESSOA',
    'PE': 'RECIFE',
    'PI': 'TERESINA',
    'PR': 'CURITIBA',
    'RJ': 'RIO DE JANEIRO',
    'RN': 'NATAL',
    'RO': 'PORTO VELHO',
    'RR': 'BOA VISTA',
    'RS': 'PORTO ALEGRE',
    'SC': 'FLORIANOPOLIS',
    'SE': 'ARACAJU',
    'SP': 'SAO PAULO',
    'TO': 'PALMAS',
}

MAP_STATE_CITY = {
    'AC': 'RIO BRANCO',
    'AL': 'MACEIO',
    'AM': 'MANAUS',
    'AP': 'MACAPA',
    'BA': 'SALVADOR',
    'CE': 'FORTALEZA',
    'DF': 'BRASILIA',
    'ES': 'VITORIA',
    'GO': 'GOIANIA',
    'MA': 'SAO LUIS',
    'MG': 'BELO HORIZONTE',
    'MS': 'CAMPO GRANDE',
    'MT': 'CUIABA',
    'PA': 'BELEM',
    'PB': 'JOAO PESSOA',
    'PE': 'RECIFE',
    'PI': 'TERESINA',
    'PR': 'CURITIBA',
    'RJ': 'RIO DE JANEIRO',
    'RN': 'NATAL',
    'RO': 'PORTO VELHO',
    'RR': 'BOA VISTA',
    'RS': 'PORTO ALEGRE',
    'SC': 'FLORIANOPOLIS',
    'SE': 'ARACAJU',
    'SP': 'SAO PAULO',
    'TO': 'PALMAS',
}


DATA_MODEL_MATERIALS_SERVICES = {
    'CREATED_AT':   pl.String,
    'ID':           pl.String,
    'GROUP':        pl.String,
    'CODE':         pl.String,
    'DESCRIPTION':  pl.String,
    'UNIT':         pl.String,
    'PRICING':      pl.String,
    'UF':           pl.String,
    'STATE':        pl.String,
    'CITY':         pl.String,
    'VALUE_TYPE':   pl.String,
    'VALUE':        pl.Float64,
    'YEAR':         pl.Int64,
    'MONTH':        pl.Int64,
    'YEAR_MONTH':   pl.Int64
}

DATA_MODEL_COMPOSITIONS = DATA_MODEL_MATERIALS_SERVICES

def add_time_columns(data: PolarsLike, year: int, month: int, /) -> pl.DataFrame:
    dataframe = transform_dataframe(data)
    dataframe= dataframe.with_columns(
        MONTH       = pl.lit(month),
        YEAR        = pl.lit(year),
        YEAR_MONTH  = pl.lit(100 * year + month)
    )
    return dataframe

def add_hash_columns(data: PolarsLike, /, *, timestamp: Optional[str]=None, token: Optional[str]=None) -> pl.DataFrame:
    if timestamp is None:
        timestamp = generate_timestamp()

    if token is None:
        token = generate_timehex_token(4)
    
    dataframe = transform_dataframe(data)
    dataframe = dataframe.with_columns(
        pl.lit(timestamp).alias('CREATED_AT'),
        pl.lit(token).alias('ID')
    )
    return dataframe

def parse_compositions_cost_headers(header_rows: PolarsLike, /) -> list[str]:
    header = transform_dataframe(header_rows)
    header = header.transpose(include_header=False, column_names=['HEAD_01', 'HEAD_02'] )
    header = forward_fill(header)

    head_01 = pl.col('HEAD_01')
    head_02 = pl.col('HEAD_02')
    
    header = header.with_columns(
        replace_whitespaces(head_01).cast(pl.String),
        replace_whitespaces(head_02).cast(pl.String)
    )
    header = header.with_columns(
        head_02.replace({
            '%AS':          'SP_PROXY_PERCENT',
            'Custo_(R$)':   'UNIT_COST_RS'
        })
    )

    not_concat_condition = any_of(
        head_01.is_null(),
        head_01.str.strip_chars() == pl.lit(''),
        lowercase_col(head_01).str.starts_with('indica'),
    )
    concat_headers = pl.concat_str(head_01, head_02, separator=';;')
    concat_rule = pl.when(not_concat_condition).then(head_02).otherwise(concat_headers)
    
    header = header.select(
        concat_rule.alias('HEADER')
    )
    header = header.with_columns(
        pl.col('HEADER').replace({
            'Grupo':                'GROUP',
            'Código_da_Composição': 'CODE',
            'Descrição':            'DESCRIPTION',
            'Unidade':              'UNIT'
        })
    )
    return header.get_column('HEADER').cast(pl.String).to_list()


def extract_materials_services_cost(data: PolarsLike, /) -> pl.DataFrame:
    dataframe = transform_dataframe(data)
    dataframe = transform_headers(dataframe, fill_whitespaces)
    
    dataframe = dataframe.rename({
        'Classificação':        'GROUP',
        'Descrição_do_Insumo':  'DESCRIPTION',
        'Unidade':              'UNIT',
        'Código_do_Insumo':     'CODE',
        'Origem_de_Preço':      'PRICING'
    })
    dataframe = dataframe.unpivot(
        index=[
            'GROUP',
            'DESCRIPTION',
            'UNIT',
            'CODE',
            'PRICING'
        ],
        variable_name='UF',
        value_name='VALUE'
    )
    dataframe = dataframe.with_columns(
        pl.lit('UNIT_COST_RS').alias('VALUE_TYPE'),
        sanitize_text('GROUP'),
        sanitize_text('DESCRIPTION'),
        sanitize_text('UNIT'),
        sanitize_text('CODE'),
        sanitize_text('PRICING'),
        sanitize_text('UF'),
        sanitize_text('VALUE'),
    )
    dataframe = dataframe.with_columns(
        pl.col('PRICING').replace({
            'C':    'COLLECTED',
            'CR':   'REPRESENTATIVENESS_COEFF'
        })
    )
    dataframe = dataframe.with_columns(
        map_elements('UF', MAP_STATE_NAME, return_dtype=pl.String, strict=True).alias('STATE'),
        map_elements('UF', MAP_STATE_CITY, return_dtype=pl.String, strict=True).alias('CITY')
    )
    dataframe = select_casting(dataframe, {
        'GROUP':        pl.String,
        'DESCRIPTION':  pl.String,
        'UNIT':         pl.String,
        'CODE':         pl.String,
        'PRICING':      pl.String,
        'UF':           pl.String,
        'STATE':        pl.String,
        'CITY':         pl.String,
        'VALUE_TYPE':   pl.String,
        'VALUE':        pl.Float64
    })
    return dataframe

def extract_compositions_cost(data: PolarsLike, /) -> pl.DataFrame:
    dataframe = transform_dataframe(data)

    header_rows = dataframe.head(2)
    headers = parse_compositions_cost_headers(header_rows)

    dataframe = skip_rows(dataframe, 2)
    dataframe.columns = headers

    dataframe = dataframe.unpivot(
        index=[
            'GROUP',
            'CODE',
            'DESCRIPTION',
            'UNIT'
        ],
        variable_name='COLUMN_NAME',
        value_name='VALUE'
    )

    parts = pl.col('COLUMN_NAME').str.split(';;', inclusive=False)
    state = parts.list[0]
    value_type = parts.list[1]
    
    dataframe = dataframe.with_columns(
        state.alias('UF'), 
        value_type.alias('VALUE_TYPE')
    )
    dataframe.drop('COLUMN_NAME')

    dataframe = dataframe.with_columns(
        sanitize_text('GROUP'),
        sanitize_text('CODE'),
        sanitize_text('DESCRIPTION'),
        sanitize_text('UNIT'),
        sanitize_text('UF'),
        sanitize_text('VALUE_TYPE'),
        sanitize_text('VALUE')
    )
    dataframe = dataframe.with_columns(
        map_elements('UF', MAP_STATE_NAME, return_dtype=pl.String, strict=True).alias('STATE'),
        map_elements('UF', MAP_STATE_CITY, return_dtype=pl.String, strict=True).alias('CITY')
    )
    dataframe = dataframe.with_columns(
        pl.col('VALUE').replace({
            '-': 0.0
        }),
        pl.lit('').alias('PRICING')
    )
    return dataframe



def load_materials_services_cost_ICD(year: int, month: int, /, *, timestamp: Optional[str]=None, token: Optional[str]=None) -> pl.DataFrame:
    '''
    Loads materials and services cost data with reduced labor charges (ICD).
    Represents SINAPI costs with payroll tax reduction applied for construction pricing.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='ICD')
    dataframe = skip_rows(dataframe, 8)
    dataframe = first_row_as_header(dataframe)
    dataframe = extract_materials_services_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_MATERIALS_SERVICES)
    return dataframe

def load_materials_services_cost_ISE(year: int, month: int, /, *, timestamp: Optional[str]=None, token: Optional[str]=None) -> pl.DataFrame:
    '''
    Loads base materials and services cost data without social charges (ISE).
    Represents pure base SINAPI prices without labor or social costs included.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='ISE')
    dataframe = skip_rows(dataframe, 7)
    dataframe = first_row_as_header(dataframe)
    dataframe = extract_materials_services_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_MATERIALS_SERVICES)
    return dataframe

def load_materials_services_cost_ISD(year: int, month: int, /, *, timestamp: Optional[str]=None, token: Optional[str]=None) -> pl.DataFrame:
    '''
    Loads materials and services cost data with full labor charges (ISD).
    Represents SINAPI costs with complete social charges included in pricing.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='ISD')
    dataframe = skip_rows(dataframe, 8)
    dataframe = first_row_as_header(dataframe)
    dataframe = extract_materials_services_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_MATERIALS_SERVICES)
    return dataframe

def load_compositions_cost_CSD(year: int, month: int, /, *, timestamp: Optional[str]=None, token: Optional[str]=None) -> pl.DataFrame:
    '''
    Loads construction composition cost data without payroll tax reduction (CSD).
    Represents SINAPI full service costs with complete labor and material charges included.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='CSD')
    dataframe = skip_rows(dataframe, 8)
    dataframe = extract_compositions_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_COMPOSITIONS)
    return dataframe
    
def load_compositions_cost_CCD(year: int, month: int, /, *, timestamp: Optional[str]=None, token: Optional[str]=None) -> pl.DataFrame:
    '''
    Loads construction composition cost data with reduced labor charges (CCD).
    Represents SINAPI service costs with payroll tax reduction applied.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='CCD')
    dataframe = skip_rows(dataframe, 8)
    dataframe = extract_compositions_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_COMPOSITIONS)
    return dataframe

def load_compositions_cost_CSE(year: int, month: int, /, *, timestamp: Optional[str]=None, token: Optional[str]=None) -> pl.DataFrame:
    '''
    Loads base construction composition cost data without social charges (CSE).
    Represents pure SINAPI composition values without labor or social costs included.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='CSE')
    dataframe = skip_rows(dataframe, 7)
    dataframe = extract_compositions_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_COMPOSITIONS)
    return dataframe

