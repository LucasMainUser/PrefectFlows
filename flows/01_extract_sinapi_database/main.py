from typing import Optional
from pydantic import BaseModel, Field

from prefect import flow, get_run_logger
from prefect.states import State
from prefect.variables import Variable

from application.envtools import load_global_environment_prefect
from core import Environment, extract_sinapi_data_to_postgres

class FlowRunConfigurations(BaseModel):
    start: str = Field(
        default=None, 
        title='Data Inicio', 
        description=(
            'Mês/Ano inicial para ler e extrair dados do SINAPI. ' 
            'Exemplo: "05/2025" para março de 2025. '
            'Deve ser informado junto com um periodo final. '
            'Se vazio, utiliza o mês anterior.' 
        )
    )
    finish: str = Field(
        default=None,  
        title='Data Fim', 
        description=(
            'Mês/Ano inicial para ler e extrair dados do SINAPI. ' 
            'Exemplo: "05/2025" para março de 2025. '
            'Deve ser informado junto com um periodo inicial. '
            'Se vazio, utiliza o mês anterior.' 
        )
    )


@flow
def prefect_flow(configurations: Optional[FlowRunConfigurations]=None) -> State:
    if configurations is None:
        configurations = FlowRunConfigurations()
    
    start = configurations.start
    finish = configurations.finish

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
        start="05/2025",
        finish="06/2026"
    )
    prefect_flow(config)

if __name__ == '__main__':
    main()

