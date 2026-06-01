from typing import Optional
from pydantic import BaseModel, Field

from prefect import flow, get_run_logger
from prefect.states import State
from prefect.variables import Variable

from application.envtools import load_global_environment_prefect
from core import Environment, extract_sinapi_data_to_postgres

class FlowRunConfigurations(BaseModel):
    start: tuple[int, int] = Field(
        default=None, 
        title='Data Inicio', 
        description='''
        (Ano, Mês) inicial para ler e extrair dados do SINAPI. Exemplo: (2025, 3) para março de 2025.
        Deve ser informado junto com um periodo final. Se vazio, utiliza o mês anterior. 
        '''
    )
    finish: tuple[int, int] = Field(
        default=None,  
        title='Data Fim', 
        description='''
        (Ano, Mês) final para ler e extrair dados do SINAPI. Exemplo: (2025, 3) para março de 2025.
        Deve ser informado junto com um periodo inicial. Se vazio, utiliza o mês anterior. 
        '''
    )

@flow
def prefect_flow(configs: Optional[FlowRunConfigurations]=None) -> State:
    if configs is None:
        configs = FlowRunConfigurations()

    start = configs.start
    finish = configs.finish

    global_environment = load_global_environment_prefect(allow_empty_values=False)
    schema = Variable.get('sinapi_database_schema')
    
    environment = Environment(
        schema=schema, global_env=global_environment)
    
    logger = get_run_logger()

    extract_sinapi_data_to_postgres(
        start=start, 
        finish=finish, 
        environment=environment, 
        logger=logger.info
    )

def main() -> None:
    config = FlowRunConfigurations(
        start=(2026, 4),
        finish=(2026, 4)
    )
    prefect_flow(config)

if __name__ == '__main__':
    main()

