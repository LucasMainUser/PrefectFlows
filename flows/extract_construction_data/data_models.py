from typing import Optional
from datetime import datetime

import polars as pl

from application.utils import fill_whitespaces, generate_timehex_token, generate_timestamp, invert_dict
from application.tables import (
    PolarsLike,
    any_of,
    transform_dataframe,
    skip_rows,
    first_row_as_header,
    transform_headers,
    nullify_cases,
    replace_whitespaces,
    forward_fill,
    lowercase_col,
    sanitize_text,
    select_casting,
    map_elements,
    cast_decimal_comma_to_float,
    drop_header_rows
)
from flows.extract_construction_data.api_sinapi import load_sinapi_dataframe
from flows.extract_construction_data.api_cub import CUBStates, NotPublishedError, load_cub_m2_table, load_cub_m2_composition

# NOTE: Maps
MAP_UF_NAME = {
    'AC': 'ACRE',
    'AL': 'ALAGOAS',
    'AM': 'AMAZONAS',
    'AP': 'AMAPA',
    'BA': 'BAHIA',
    'CE': 'CEARA',
    'DF': 'DISTRITO FEDERAL',
    'ES': 'ESPIRITO SANTO',
    'GO': 'GOIAS',
    'MA': 'MARANHAO',
    'MG': 'MINAS GERAIS',
    'MS': 'MATO GROSSO DO SUL',
    'MT': 'MATO GROSSO',
    'PA': 'PARA',
    'PB': 'PARAIBA',
    'PE': 'PERNAMBUCO',
    'PI': 'PIAUI',
    'PR': 'PARANA',
    'RJ': 'RIO DE JANEIRO',
    'RN': 'RIO GRANDE DO NORTE',
    'RO': 'RONDONIA',
    'RR': 'RORAIMA',
    'RS': 'RIO GRANDE DO SUL',
    'SC': 'SANTA CATARINA',
    'SE': 'SERGIPE',
    'SP': 'SAO PAULO',
    'TO': 'TOCANTINS',
}

MAP_UF_CAPITAL = {
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

MAP_PROJECT_TYPE = {
    'R':        'RESIDENTIAL_BUILDING',
    'PP':       'LOW_COST_APARTMENTS',
    'PIS':      'SOCIAL_HOUSING',
    'CAL':      'COMMERCIAL_OPEN_PLAN',
    'CSL':      'COMMERCIAL_RETAIL_AND_OFFICES',
    'RP1Q':     'LOW_COST_HOUSING',
    'GI':       'INDUSTRIAL_BUILDING',
}

MAP_PROJECT_STANDARD = {
    'B':    'LOW',
    'N':    'STANDARD',
    'A':    'HIGH',
}


# NOTE: Schemas (models)
DATA_MODEL_MATERIALS_SERVICES = {
    'CREATED_AT':   pl.Datetime,
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

DATA_MODEL_CUB_M2 = {
    'ID':                       pl.String,

    'UF':                       pl.String,
    'CITY':                     pl.String,
    'STATE':                    pl.String,

    'PROJECT_CODE':             pl.String,
    'PROJECT_TYPE':             pl.String,
    'PROJECT_FLOOR':            pl.String,
    'PROJECT_STANDARD':         pl.String,

    'CUB_BRL_M2':               pl.Float64,
    'CUB_VARIATION_PERCENT':    pl.Float64,

    'MONTH':                    pl.Int64,
    'YEAR':                     pl.Int64,
    'YEAR_MONTH':               pl.Int64,

    'CREATED_AT':               pl.Datetime,
}

DATA_MODEL_CUB_COMPOSITION = {
    'ID':                       pl.String,
    'COMPONENT':                pl.String,

    'UF':                       pl.String,
    'CITY':                     pl.String,
    'STATE':                    pl.String,
    
    'PROJECT_CODE':             pl.String,
    'PROJECT_TYPE':             pl.String,
    'PROJECT_FLOOR':            pl.String,
    'PROJECT_STANDARD':         pl.String,

    'VALUE_BRL_PER_M2':         pl.Float64,
    
    'MONTH':                    pl.Int64,
    'YEAR':                     pl.Int64,
    'YEAR_MONTH':               pl.Int64,

    'CREATED_AT':               pl.Datetime
}


def add_time_columns(data: PolarsLike, year: int, month: int, /) -> pl.DataFrame:
    dataframe = transform_dataframe(data)
    dataframe = dataframe.with_columns(
        MONTH=pl.lit(month),
        YEAR=pl.lit(year),
        YEAR_MONTH=pl.lit(100 * year + month)
    )
    return dataframe

def add_hash_columns(data: PolarsLike, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
    if timestamp is None:
        timestamp = generate_timestamp()

    if token is None:
        token = generate_timehex_token(4)

    dataframe = transform_dataframe(data)
    dataframe = dataframe.with_columns(
        pl.lit(timestamp, dtype=pl.Datetime).alias('CREATED_AT'),
        pl.lit(token, dtype=pl.String).alias('ID')
    )
    return dataframe

def add_uf_columns(data: PolarsLike, uf_column: str, /) -> pl.DataFrame:
    dataframe = transform_dataframe(data).with_columns(
        map_elements(uf_column, MAP_UF_NAME, return_dtype=pl.String, strict=True).alias('STATE'),
        map_elements(uf_column, MAP_UF_CAPITAL, return_dtype=pl.String, strict=True).alias('CITY')
    )
    return dataframe

def add_project_columns(data: PolarsLike, project_code_column: str, /) -> pl.DataFrame:
    dataframe = transform_dataframe(data)
    
    dataframe = dataframe.with_columns(
        pl.col(project_code_column).alias('PROJECT_CODE')
    )
    dataframe = dataframe.with_columns(
        pl.col('PROJECT_CODE').str.splitn('-', n=3).alias('PARTS')
    )
    dataframe = dataframe.unnest('PARTS')
    dataframe = dataframe.rename({
        'field_0': 'PROJECT_TYPE', 
        'field_1': 'PROJECT_FLOOR', 
        'field_2': 'PROJECT_STANDARD'
    })
    dataframe = dataframe.with_columns(
        map_elements('PROJECT_TYPE', MAP_PROJECT_TYPE, return_dtype=pl.String, strict=True, skip_nulls=False),
        map_elements('PROJECT_STANDARD', MAP_PROJECT_STANDARD, return_dtype=pl.String, strict=True, skip_nulls=False),
    )
    dataframe = dataframe.with_columns(
        sanitize_text('PROJECT_TYPE'),
        sanitize_text('PROJECT_FLOOR'),
        sanitize_text('PROJECT_STANDARD'),
    )
    return dataframe


def parse_compositions_cost_headers(header_rows: PolarsLike, /) -> list[str]:
    header = transform_dataframe(header_rows)
    header = header.transpose(include_header=False, column_names=[
                              'HEAD_01', 'HEAD_02'])
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
    concat_rule = pl.when(not_concat_condition).then(
        head_02).otherwise(concat_headers)

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
    dataframe = select_casting(dataframe, {
        'GROUP':        pl.String,
        'DESCRIPTION':  pl.String,
        'UNIT':         pl.String,
        'CODE':         pl.String,
        'PRICING':      pl.String,
        'UF':           pl.String,
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
        pl.col('VALUE').replace({
            '-': 0.0
        }),
        pl.lit('').alias('PRICING')
    )
    return dataframe


# NOTE: Model factorys
def load_model_materials_services_cost_ICD(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
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
    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = select_casting(dataframe, DATA_MODEL_MATERIALS_SERVICES)
    return dataframe

def load_model_materials_services_cost_ISE(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
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
    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = select_casting(dataframe, DATA_MODEL_MATERIALS_SERVICES)
    return dataframe

def load_model_materials_services_cost_ISD(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
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
    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = select_casting(dataframe, DATA_MODEL_MATERIALS_SERVICES)
    return dataframe

def load_model_compositions_cost_CSD(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
    '''
    Loads construction composition cost data without payroll tax reduction (CSD).
    Represents SINAPI full service costs with complete labor and material charges included.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='CSD')
    dataframe = skip_rows(dataframe, 8)
    dataframe = extract_compositions_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = select_casting(dataframe, DATA_MODEL_COMPOSITIONS)
    return dataframe

def load_model_compositions_cost_CCD(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
    '''
    Loads construction composition cost data with reduced labor charges (CCD).
    Represents SINAPI service costs with payroll tax reduction applied.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='CCD')
    dataframe = skip_rows(dataframe, 8)
    dataframe = extract_compositions_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = select_casting(dataframe, DATA_MODEL_COMPOSITIONS)
    return dataframe

def load_model_compositions_cost_CSE(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
    '''
    Loads base construction composition cost data without social charges (CSE).
    Represents pure SINAPI composition values without labor or social costs included.
    '''
    dataframe = load_sinapi_dataframe(year=year, month=month, sheet_name='CSE')
    dataframe = skip_rows(dataframe, 7)
    dataframe = extract_compositions_cost(dataframe)
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = select_casting(dataframe, DATA_MODEL_COMPOSITIONS)
    return dataframe

def load_model_cub_m2(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
    '''
    Loads CUB cost per square meter (BRL/m²) for multiple Brazilian states (UFs).
    Data sourced from Sinduscon reports, including value and monthly variation.
    '''
    dataframes: list[pl.DataFrame] = []

    for uf in CUBStates:
        try:
            cub_m2_dataframe = load_cub_m2_table(uf, year, month)
        except (NotPublishedError, ValueError):
            continue
        
        cub_m2_dataframe = cub_m2_dataframe.with_columns(
            pl.lit(uf).alias('UF')
        )
        dataframes.append(cub_m2_dataframe)

    if not dataframes:
        return pl.DataFrame(schema=DATA_MODEL_CUB_M2)
    
    dataframe = pl.concat(dataframes)
    dataframe = dataframe.rename({
        'column_0': 'PROJECT_CODE',
        'column_1': 'CUB_BRL_M2',
        'column_2': 'CUB_VARIATION_PERCENT'
    })
    missing_cub_values = pl.col('CUB_BRL_M2').is_null() | pl.col('CUB_VARIATION_PERCENT').is_null()
    
    dataframe = dataframe.with_columns(
        pl.when(missing_cub_values).then('PROJECT_CODE').otherwise(None).alias('PROJECT_STANDARD')
    )
    dataframe = dataframe.with_columns(
        pl.col('PROJECT_STANDARD').forward_fill()
    )
    dataframe = dataframe.filter(~missing_cub_values)
    dataframe = dataframe.with_columns(
        pl.col('CUB_VARIATION_PERCENT').str.replace_all('%', '')
    )
    dataframe = dataframe.with_columns(
        sanitize_text('UF'),
        sanitize_text('PROJECT_CODE'),
        sanitize_text('PROJECT_STANDARD'),
        sanitize_text('CUB_BRL_M2'),
        sanitize_text('CUB_VARIATION_PERCENT'),
    )
    dataframe = dataframe.with_columns(
        nullify_cases('CUB_BRL_M2', '-', return_dtype=pl.String),
        nullify_cases('CUB_VARIATION_PERCENT', '-', return_dtype=pl.String),
        
        pl.col('PROJECT_STANDARD').replace_strict({
            'PADRÃO BAIXO':     'B',
            'PADRÃO NORMAL':    'N',
            'PADRÃO ALTO':      'A'
        })
    )
    dataframe = dataframe.with_columns(
        cast_decimal_comma_to_float('CUB_BRL_M2'),
        cast_decimal_comma_to_float('CUB_VARIATION_PERCENT'),        
    )
    dataframe = dataframe.with_columns(
        pl.when(
            pl.col('PROJECT_CODE').is_in({'RP1Q', 'GI', 'PIS'})
        ).then(
            pl.concat_str('PROJECT_CODE', pl.lit('1'), pl.lit('N'), separator='-', ignore_nulls=False)
        ).otherwise(
            pl.concat_str('PROJECT_CODE', 'PROJECT_STANDARD', separator='-', ignore_nulls=False)
        )
    )
    dataframe = dataframe.drop('PROJECT_STANDARD')

    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = add_project_columns(dataframe, 'PROJECT_CODE')
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_CUB_M2)
    return dataframe

def load_model_cub_m2_composition(year: int, month: int, /, *, timestamp: Optional[datetime] = None, token: Optional[str] = None) -> pl.DataFrame:
    '''
    Loads CUB cost composition per square meter (BRL/m²) for a Brazilian state (UF).
    Data sourced from Sinduscon, detailing cost components (materials, labor, etc.).
    '''
    dataframes: list[pl.DataFrame] = []

    for uf in CUBStates:
        try:
            cub_composition_data = load_cub_m2_composition(uf, year, month)
        except (NotPublishedError, ValueError):
            continue
        cub_composition_data = first_row_as_header(cub_composition_data)
        cub_composition_data = drop_header_rows(cub_composition_data)
        cub_composition_data = cub_composition_data.with_columns(
            pl.lit(uf).alias('UF')
        )
        dataframes.append(cub_composition_data)

    if not dataframes:
        return pl.DataFrame(schema=DATA_MODEL_CUB_COMPOSITION)
    
    dataframe = pl.concat(dataframes)
    dataframe = dataframe.rename({
        'Item': 'PROJECT_CODE'
    })
    dataframe = dataframe.unpivot(
        index=['UF', 'PROJECT_CODE'],  
        value_name='VALUE_BRL_PER_M2',
        variable_name='COMPONENT',
    )
    dataframe = dataframe.with_columns(
        nullify_cases('VALUE_BRL_PER_M2', '-', return_dtype=pl.String)
    )
    dataframe = dataframe.with_columns(
        sanitize_text('UF'),
        sanitize_text('PROJECT_CODE'),
        sanitize_text('COMPONENT'),
        cast_decimal_comma_to_float('VALUE_BRL_PER_M2')
    )
    dataframe = dataframe.with_columns(
        pl.when(
            pl.col('PROJECT_CODE').is_in({'RP1Q', 'GI', 'PIS'})
        ).then(
            pl.concat_str('PROJECT_CODE', pl.lit('1'), pl.lit('N'), separator='-', ignore_nulls=False)
        ).otherwise(
            pl.col('PROJECT_CODE')
        )
    )
    dataframe = dataframe.with_columns(
        pl.col('PROJECT_CODE').str.replace_many({
            'R1':   'R-1',
            'R8':   'R-8',
            'R16':  'R-16',
        })
    )
    dataframe = add_uf_columns(dataframe, 'UF')
    dataframe = add_project_columns(dataframe, 'PROJECT_CODE')
    dataframe = add_time_columns(dataframe, year, month)
    dataframe = add_hash_columns(dataframe, timestamp=timestamp, token=token)
    dataframe = select_casting(dataframe, DATA_MODEL_CUB_COMPOSITION)
    return dataframe
