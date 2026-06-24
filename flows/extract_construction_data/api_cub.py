from __future__ import annotations

from typing import (
    Any, 
    Optional, 
    Mapping, 
    Iterator, 
    Hashable,
    Callable,
    NoReturn
)

import io
import enum as en
import dataclasses as dc

import bs4 as bs
import polars as pl
import requests as rq
import pdfplumber as pb
import pdfplumber.pdf as pdf

from application.utils import cache, invert_dict, sign_with


class CUBEndpoints(en.StrEnum):
    REPORT_GENERATION = r'http://www.cub.org.br/cub-m2-estadual/'

class CUBStates(en.StrEnum):
    AC = 'AC'
    AM = 'AM'
    BA = 'BA'
    CE = 'CE'
    DF = 'DF'
    ES = 'ES'
    GO = 'GO'
    MA = 'MA'
    MT = 'MT'
    MG = 'MG'
    PR = 'PR'
    PB = 'PB'
    PA = 'PA'
    PE = 'PE'
    PI = 'PI'
    RN = 'RN'
    RJ = 'RJ'
    RO = 'RO'
    RR = 'RR'
    SC = 'SC'
    SE = 'SE'

class CUBReports(en.StrEnum):
    MEDIAN_PRICES = 'precos-medianos'
    CUB_M2_TABLE = 'tabela-cub-m2'
    CUB_M2_COMPOSITION = 'composicao-cub-m2'
    COST_EVOLUTION = 'evolucao-custos'
    PRICE_EVOLUTION_12M = 'evolucao-precos'
    PRICE_VARIATION_12M = 'variacao-percentual-precos-12-meses'

@dc.dataclass(slots=True, frozen=True)
class CUBSessionContext:
    session: rq.Session
    csrf_token: str

@dc.dataclass(slots=True, frozen=True)
class CUBSchema:
    sinduscon_state: str
    sinduscon_state_name: str
    sinduscon_name: str
    sinduscon_tag: int
    available_projects: dict[int, str]
    available_reports: list[str]
    available_years: list[int]
    available_months: list[int]

    @property
    def identidy(self) -> Hashable:
        return (self.sinduscon_tag, self.sinduscon_name, self.sinduscon_state)
    
    def __eq__(self, other: Any, /) -> bool:
        return isinstance(other, CUBSchema) and self.identidy == other.identidy

    def __hash__(self) -> int:
        return hash(self.identidy)

    def get_project_from_name(self, project_name: str, /) -> int:
        available_project_names = invert_dict(self.available_projects)
        if project_name not in available_project_names:
            available_project_names = list(available_project_names)
            raise ValueError(
                f'[{self.sinduscon_name}] Invalid project name: {project_name!r}. '
                f'Available: {available_project_names}'
            )
        project = available_project_names[project_name]
        return int(project)

    def assert_is_valid_report(self, report: str, /) -> None:
        if report not in self.available_reports:
            raise ValueError(
                f'[{self.sinduscon_name}] Invalid report: {report!r}. '
                f'Available: {self.available_reports}'
            )

    def assert_is_valid_year(self, year: int, /) -> None:
        if year not in self.available_years:
            raise ValueError(
                f'[{self.sinduscon_name}] Invalid year: {year}. '
                f'Available: {self.available_years}'
            )

    def assert_is_valid_month(self, month: int, /) -> None:
        if month not in self.available_months:
            raise ValueError(
                f'[{self.sinduscon_name}] Invalid month: {month}. '
                f'Available: {self.available_months}'
            )

    def assert_is_valid_project(self, project: int, /) -> None:
        if project not in self.available_projects:
            available_projects = list(available_projects)
            raise ValueError(
                f'[{self.sinduscon_name}] Invalid project id: {project}. '
                f'Available: {available_projects}'
            )

class NotPDFError(Exception): ...

class NotPublishedError(Exception): ...

    

def extract_csrf_token(html: str, /) -> str:
    parser = bs.BeautifulSoup(html, 'html.parser')
    token_input = parser.select_one('input[name=csrfmiddlewaretoken]')

    if token_input is None or not token_input.get('value'):
        raise RuntimeError('Cannot find CSRF token from html page')

    csrf_token = token_input.get('value')
    return str(csrf_token)

def find_html_select(parser: bs.BeautifulSoup, identifier: str, /) -> bs.Tag:
    selection = parser.find('select', {'id': identifier})
    if selection is None:
        raise RuntimeError(
            f'Select element with id {identifier!r} not found in HTML')
    return selection

def init_cub_session_context(session: rq.Session, /, configure_session: bool = False) -> CUBSessionContext:
    endpoint = CUBEndpoints.REPORT_GENERATION
    endpoint = str(endpoint)    

    response = session.get(endpoint)
    response.raise_for_status()

    csrf_token = extract_csrf_token(response.text)
    context = CUBSessionContext(session=session, csrf_token=csrf_token)

    if configure_session:
        session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Origin': 'http://www.cub.org.br',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': endpoint,
            'X-CSRFToken': csrf_token
        })
    return context

def extract_tables_each_page(pdf: pdf.PDF, /, table_settings: Optional[Any]=None) -> Iterator[list[list] ]:
    for page in pdf.pages:
        yield from page.extract_tables(table_settings)

def is_sinduscon_report_not_published(content: bytes | str, /) -> bool:
    flag = 'Erro: Relatório ainda não foi publicado'
    if isinstance(content, str):
        return flag in content
    return flag.encode('utf-8') in content

CUB_SESSION = rq.Session()
CUB_SESSION_CONTEXT = init_cub_session_context(CUB_SESSION, configure_session=True)

@cache
def generate_cub_uf_endpoint(uf: CUBStates | str, /) -> str:
    base_endpoint = CUBEndpoints.REPORT_GENERATION
    base_endpoint = base_endpoint.rstrip('/')
    return f'{base_endpoint}/{uf}/'

@cache
def extract_cub_schema(data: CUBSchema | CUBStates | str, /) -> CUBSchema:
    if isinstance(data, CUBSchema):
        return data

    state = str(data)
    endpoint = generate_cub_uf_endpoint(state)

    response = CUB_SESSION_CONTEXT.session.get(endpoint)
    response.raise_for_status()

    parser = bs.BeautifulSoup(response.text, 'html.parser')

    states_select = find_html_select(parser, 'uf')
    sinduscons_select = find_html_select(parser, 'sinduscon')
    projects_select = find_html_select(parser, 'projeto')
    reports_select = find_html_select(parser, 'relatorio')
    years_select = find_html_select(parser, 'ano')
    months_select = find_html_select(parser, 'mes')

    state_option = states_select.select_one('option[selected]')
    sinduscon_option = None

    sinduscons_options = sinduscons_select.find_all('option')
    sinduscons_options = list(sinduscons_options)
    sinduscons_options_count = len(sinduscons_options)

    if sinduscons_options_count == 1:
        sinduscon_option = sinduscons_options[0]

    if sinduscons_options_count > 1:
        sinduscon_option = next(
            option for option in sinduscons_options if option.text == f'Sinduscon-{data}')

    if not isinstance(sinduscon_option, bs.Tag):
        raise ValueError(
            f'Sinduscon unique-option for {data!r} not found in HTML'
        )

    if not isinstance(state_option, bs.Tag):
        raise ValueError(
            f'{data!r} not found in HTML. Is it a valid sinduscon state? see {endpoint!r}.'
        )

    sinduscon_state = state_option.get('value')
    sinduscon_tag = sinduscon_option.get('value')
    sinduscon_state_name = state_option.text.strip()
    sinduscon_name = sinduscon_option.text.strip()
    available_projects = {
        int(option.get('value')): option.text.strip() for option in projects_select.find_all('option') if option.get('value')
    }
    available_reports = [
        str(option.get('value')) for option in reports_select.find_all('option') if option.get('value')
    ]
    available_years = [
        int(option.get('value')) for option in years_select.find_all('option') if option.get('value')
    ]
    available_months = [
        int(option.get('value')) for option in months_select.find_all('option') if option.get('value')
    ]

    if sinduscon_state is None:
        raise RuntimeError('UF value is missing in selected option')

    if sinduscon_tag is None:
        raise RuntimeError('Sinduscon tag value is missing in selected option')

    if not sinduscon_name:
        raise RuntimeError('Sinduscon name is empty in HTML')

    if not available_projects:
        raise RuntimeError('No projects available in CUB HTML')

    if not available_reports:
        raise RuntimeError('No reports available in CUB HTML')

    if not available_years:
        raise RuntimeError('No years available in CUB HTML')

    if not available_months:
        raise RuntimeError('No months available in CUB HTML')

    schema = CUBSchema(
        sinduscon_state=str(sinduscon_state),
        sinduscon_state_name=str(sinduscon_state_name),
        sinduscon_name=str(sinduscon_name),
        sinduscon_tag=int(sinduscon_tag),
        available_projects=dict(available_projects),
        available_reports=list(available_reports),
        available_years=list(available_years),
        available_months=list(available_months)
    )
    return schema

def post_cub_pdf_request(uf: CUBStates | str, payload: Mapping[str, str], /) -> bytes:
    endpoint = generate_cub_uf_endpoint(uf)

    response = CUB_SESSION_CONTEXT.session.post(endpoint, data=dict(payload))
    response.raise_for_status()

    if is_sinduscon_report_not_published(response.content):
        raise NotPublishedError(f'Report for UF {uf!r} has not been published yet. Please, check the playload.')

    if not response.headers.get('Content-Type', '').startswith('application/pdf'):
        raise NotPDFError(
            f'CUB response for UF {uf!r} is not a PDF.'
            f'See for response:  Status={response.status_code} | Headers={response.headers}'
        )
    return response.content

def fetch_cub_report_pdf(
        uf_or_schema: CUBSchema | CUBStates | str,
        report: str,
        year: Optional[int]=None,
        month: Optional[int]=None,
        start_year: Optional[int]=None,
        end_year: Optional[int]=None,
        project: Optional[int | str]=None,
    ) -> bytes:
    
    schema = extract_cub_schema(uf_or_schema)
    schema.assert_is_valid_report(report)
    
    payload = {
        'csrfmiddlewaretoken': str(CUB_SESSION_CONTEXT.csrf_token),
        'uf': str(schema.sinduscon_state),
        'sinduscon': str(schema.sinduscon_tag),
        'relatorio': str(report),   
        'desoneracao': 'sem-desoneracao',
        'variacao': 'com-variacao',
        'cimento': '2'
    }

    if report == CUBReports.COST_EVOLUTION:
        if project is None:
            raise ValueError(f'project is required when report is {report!r}.')
        
        if isinstance(project, str):
            project = schema.get_project_from_name(project)

        if start_year is None:
            raise ValueError(f'start_year is required when report is {report!r}.')

        if end_year is None:
            raise ValueError(f'end_year is required when report is {report!r}')

        if start_year > end_year:
            raise ValueError(
                f'Range error. start_year ({start_year}) cannot be greater than end_year ({end_year}).')

        schema.assert_is_valid_year(start_year)
        schema.assert_is_valid_year(end_year)
        schema.assert_is_valid_project(project)

        payload['ano_i'] = str(start_year)
        payload['ano_f'] = str(end_year)
        payload['projeto'] = str(project)

        return post_cub_pdf_request(schema.sinduscon_state, payload)

    if year is None:
        raise ValueError(f'year is required when project is {report!r}')
    
    if month is None:
        raise ValueError(f'month is required when project is {report!r}')

    schema.assert_is_valid_year(year)
    schema.assert_is_valid_month(month)

    payload['ano'] = str(year)
    payload['mes'] = str(month)

    return post_cub_pdf_request(schema.sinduscon_state, payload)

@sign_with(fetch_cub_report_pdf)
def open_cub_report_pdf(*args, **kwargs) -> pdf.PDF:
    data = fetch_cub_report_pdf(*args, **kwargs)
    if isinstance(data, io.BytesIO):
        return pb.open(data)
    return pb.open(io.BytesIO(data))


def load_cub_m2_table(uf_or_schema: CUBSchema | CUBStates | str, year: int, month: int, /) -> pl.DataFrame:
    with open_cub_report_pdf(uf_or_schema=uf_or_schema, year=year, month=month, report=CUBReports.CUB_M2_TABLE) as pdf:
        dataframes = (
            pl.DataFrame(data, orient='row') for data in extract_tables_each_page(pdf)
        )
    return pl.concat(dataframes)

def load_cub_m2_composition(uf_or_schema: CUBSchema | CUBStates | str, year: int, month: int, /) -> pl.DataFrame:
    with open_cub_report_pdf(uf_or_schema=uf_or_schema, year=year, month=month, report=CUBReports.CUB_M2_COMPOSITION) as pdf:
        dataframes = (
            pl.DataFrame(data, orient='col') for data in extract_tables_each_page(pdf)
        )
    return pl.concat(dataframes)

