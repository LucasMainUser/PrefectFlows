from typing import Optional
from pydantic import BaseModel, Field

from prefect import flow, get_run_logger
from prefect.states import State
from prefect.variables import Variable

from application.envtools import load_global_environment_prefect
from core import Environment, extract_construction_data

class RunConfigurations(BaseModel):
    start: str = Field(
        default=None, 
        title='Data Inicio', 
        description=(
            'Mês/Ano inicial para ler e extrair dados do SINAPI. ' 
            'Exemplo: "05/2025" para março de 2025. '
            'Deve ser informado junto com um periodo final. '
            'Se vazio, utiliza o mês anterior ao atual.' 
        )
    )
    finish: str = Field(
        default=None,  
        title='Data Fim', 
        description=(
            'Mês/Ano final para ler e extrair dados do SINAPI. ' 
            'Exemplo: "05/2025" para março de 2025. '
            'Deve ser informado junto com um periodo inicial. '
            'Se vazio, utiliza o mês anterior ao atual.' 
        )
    )
    update_latest_only: bool = Field(
        default=False,  
        title='Apenas Atualizar Ultimos', 
        description=(
            'Quando ativado, apenas atualiza os registros mais atuais. ' 
            'Não realiza extração de dados do SINAPI, apenas utiliza os dados ja existentes.'
        )
    )
    

@flow
def prefect_flow(configurations: Optional[RunConfigurations]=None) -> State:
    if configurations is None:
        configurations = RunConfigurations()
    
    start = configurations.start
    finish = configurations.finish
    update_latest_only = configurations.update_latest_only

    global_environment = load_global_environment_prefect(allow_empty_values=False)
    
    environment_values = Variable.get('construction_environment', {})
    environment_values = dict(environment_values)

    schema = environment_values['TABLES_SCHEMA']
    bucket_name = environment_values['S3_BUCKET_NAME']
    ignore_sinapi = environment_values['IGNORE_SINAPI']
    ignore_cub = environment_values['IGNORE_CUB']
    
    environment = Environment(
        schema=str(schema), 
        bucket_name=str(bucket_name), 
        ignore_sinapi=bool(ignore_sinapi),
        ignore_cub=bool(ignore_cub),
        global_env=global_environment)
    
    logger = get_run_logger()
    
    extract_construction_data(
        start=start, 
        finish=finish, 
        environment=environment,
        update_latest_only=update_latest_only,
        logger=logger.info)

def main() -> None:
    start='01/2026'
    finish='05/2026'
    update_latest_only = True

    extract_construction_data(
        start=start, 
        finish=finish,
        update_latest_only=update_latest_only,
        logger=print)

if __name__ == '__main__':
    main()

