from typing import NamedTuple, Optional

from prefect import deploy, flow
from prefect.deployments.runner import RunnerDeployment
from prefect.runner.storage import RunnerStorage, GitRepository


TIMEZONE:           str = 'America/Fortaleza'
WORKPOOL_NAME:      str = 'wp'
GITHUB_REPOSITORY:  str = 'https://github.com/LucasMainUser/PrefectFlows.git'

class DeploySpec(NamedTuple):
    name: str
    flow_name: str
    entrypoint: str
    storage: RunnerStorage
    cron: Optional[str]=None
    paused: bool=False

def to_prefect_deployment(order: DeploySpec, /) -> RunnerDeployment:
    flow_instance = flow.from_source(
        source=order.storage, entrypoint=order.entrypoint)
    
    flow_instance = flow_instance.with_options(name=order.flow_name)
    
    flow_deployment = flow_instance.to_deployment(
        name=order.name,
        cron=order.cron,
        paused=order.paused
    )
    return flow_deployment


def main() -> None:
    github = GitRepository(url=GITHUB_REPOSITORY, branch='master')
    
    orders: list[DeploySpec] = [
        DeploySpec('Extrair Dados SINAPI', flow_name='extrair_dados_sinapi', entrypoint=r'flows/01_extract_sinapi_database/main.py:prefect_flow', storage=github, paused=False, cron='0 6 28 * *')
    ]

    deployments = map(to_prefect_deployment, orders)
    deploy(*deployments, work_pool_name=WORKPOOL_NAME)

if __name__ == '__main__':
    main()
