import dataclasses
import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from time import sleep, time
from typing import Self

from aiven.client.client import AivenClient, Error
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client

from clickhouse_benchmark.config import Config

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Service:
    service_name: str
    plan: str
    client: Client

    @classmethod
    def from_running_service(cls, service: dict) -> Self:
        port = next(
            c["port"]
            for c in service["components"]
            if c["component"] == "clickhouse_https"
        )
        return cls(
            service_name=service["service_name"],
            plan=service["plan"],
            client=get_client(
                host=service["service_uri_params"]["host"],
                port=port,
                user=service["service_uri_params"]["user"],
                password=service["service_uri_params"]["password"],
                database=service["service_uri_params"]["dbname"],
                interface="https",
                connect_timeout=60 * 5,
                send_receive_timeout=60 * 5,
            ),
        )


def create_aiven_client() -> AivenClient:
    url = os.getenv("AIVEN_WEB_URL")
    assert url
    creds_file = os.getenv("AIVEN_CREDENTIALS_FILE")
    assert creds_file
    with open(creds_file, "r") as file:
        auth_token = json.load(file)["auth_token"]
    client = AivenClient(base_url=url)
    client.set_auth_token(auth_token)
    return client


@contextmanager
def create_services(client: AivenClient, config: Config) -> Iterator[Iterator[Service]]:
    service_names = []
    try:
        for plan in config.plans:
            LOG.info("Creating service %s", plan)
            service_names.append(get_or_create_service(client, plan, config))
        yield map(
            Service.from_running_service,
            wait_for_services_to_become_running(client, service_names, config.project),
        )
    finally:
        if config.terminate_services_on_exit:
            terminate_services(client, service_names, config.project)


def get_or_create_service(client: AivenClient, plan: str, config: Config) -> str:
    service_name = plan
    try:
        return client.get_service(config.project, service_name)["service_name"]
    except Error as e:
        if e.status != 404:
            raise e from e
        LOG.info("Creating service %s", plan)
        return client.create_service(
            cloud=config.cloud,
            service_type="clickhouse",
            plan=plan,
            project=config.project,
            service=plan,
        )["service_name"]


def wait_for_services_to_become_running(
    client: AivenClient, service_names: list[str], project: str, timeout: float = 600.0
) -> Iterator[dict]:
    start_time = time()
    while time() - start_time < timeout:
        remaining_services = []
        for service_name in service_names:
            service = client.get_service(project, service_name)
            if service["state"] == "RUNNING":
                LOG.info("Service %s is running", service_name)
                yield service
            else:
                remaining_services.append(service_name)
                LOG.info("Service %s is %s", service_name, service["state"])
        service_names = remaining_services
        if not service_names:
            return
        sleep(5)


def terminate_services(
    client: AivenClient, service_names: list[str], project: str
) -> None:
    for service_name in service_names:
        LOG.info("Terminating service %s", service_name)
        client.delete_service(project, service_name)
