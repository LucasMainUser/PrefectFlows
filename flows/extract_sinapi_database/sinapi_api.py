from typing import (
    Any,
    Mapping, 
    Optional
)
from enum import StrEnum

import io
import zipfile

import polars as pl
import requests as rq

from application.utils import cache, search
from application.tables import lookup

class Endpoints_SINAPI(StrEnum): 
    AVAILABLE_LINKS = r'https://www.caixa.gov.br/_api/web/lists/Downloads/Items'

SESSION = rq.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0',
    'Accept': '*/*'
})

@cache
def load_available_links_sinapi(headers: Optional[Mapping]=None) -> pl.DataFrame:
    if headers is None:
        headers = {}
    
    headers = dict(headers)
    
    headers.setdefault('Accept', 'application/json;odata=verbose')
    headers.setdefault('User-Agent', 'Mozilla/5.0')

    params = {
        '$select':  'Title,EncodedAbsUrl',
        '$filter':  'Categoria/ID eq 888 and FSObjType eq 0 and OData__ModerationStatus eq 0'
    }

    response = SESSION.get(
        url=Endpoints_SINAPI.AVAILABLE_LINKS, 
        params=params, 
        headers=headers,
        allow_redirects=True
    )
    response.raise_for_status()

    content: dict[str, Any] = response.json()
    
    dataframe = content['d']['results']

    dataframe = pl.DataFrame(dataframe)
    dataframe = dataframe.rename({
        'Title': 'TITLE',
        'EncodedAbsUrl': 'LINK'
    })
    dataframe = dataframe.select('TITLE', 'LINK')

    title_column = pl.col('TITLE')

    dataframe = dataframe.filter(
        title_column.str.contains('xlsx') & 
        title_column.str.starts_with('SINAPI')
    )
    dataframe = dataframe.with_columns(
        title_column.str.replace_all('_', '-', literal=True)
    )
    dataframe = dataframe.with_columns(
        title_column.str.split('-').list[1].alias('YEAR'),
        title_column.str.split('-').list[2].alias('MONTH')
    )
    dataframe = dataframe.cast({
        'TITLE':        pl.String,
        'LINK':         pl.String,
        'YEAR':         pl.Int64,
        'MONTH':        pl.Int64,
    })
    return dataframe

@cache
def get_link_to_sinapi_table(year: int, month: int, /, headers: Optional[Mapping]=None) -> str:
    available_links = load_available_links_sinapi(headers=headers)
    
    selected_link = lookup(
        available_links, {'YEAR': year, 'MONTH': month}, at_column='LINK')
    
    return str(selected_link)

@cache
def is_available_sinapi_data(year: int, month: int, /) -> bool:
    try:
        get_link_to_sinapi_table(year, month)
        return True
    except Exception:
        return False

def load_sinapi_dataframe(
        year: int, 
        month: int,
        sheet_name: Optional[str]=None,
        headers: Optional[Mapping]=None
    ) -> pl.DataFrame:
    
    selected_link = get_link_to_sinapi_table(year, month, headers=headers)
    
    response = SESSION.get(selected_link, allow_redirects=True, stream=True, timeout=60)
    response.raise_for_status()
    
    zip_bytes = io.BytesIO(response.content)
    
    with zipfile.ZipFile(zip_bytes) as zip_stream:
        available_filenames = zip_stream.namelist()
        is_sinapi_reference_table = lambda name: ('xlsx' in name) and ('SINAPI_Referência' in name)
        
        try:
            filename = search(available_filenames, is_sinapi_reference_table)
        except ValueError:
            raise FileNotFoundError(
                f'No SINAPI excel-file was found in the ZIP archive to year={year} and month={month}. '
                f'Available filenames: {available_filenames}'
            ) 

        with zip_stream.open(filename, mode='r') as excel_stream:
            dataframe = pl.read_excel(source=excel_stream.read(), engine='calamine', sheet_name=sheet_name, has_header=False)

    return dataframe

