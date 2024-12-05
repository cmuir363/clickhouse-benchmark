import dataclasses
import json
import logging
import os
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from time import sleep, time
from typing import Any, Self, TypeAlias

from aiven.client.client import AivenClient, Error

from clickhouse_benchmark.client import ClickHouseClient
from clickhouse_benchmark.config import Config

LOG = logging.getLogger(__name__)

ServiceDict: TypeAlias = dict[str, Any]


@dataclasses.dataclass
class Service:
    service_name: str
    plan: str
    client: ClickHouseClient

    @classmethod
    def from_running_service(cls, service: ServiceDict) -> Self:
        port = next(
            c["port"] for c in service["components"] if c["component"] == "clickhouse"
        )
        return cls(
            service_name=service["service_name"],
            plan=service["plan"],
            client=ClickHouseClient(
                host=service["service_uri_params"]["host"],
                port=port,
                user=service["service_uri_params"]["user"],
                password=service["service_uri_params"]["password"],
                database=service["service_uri_params"]["dbname"],
                secure=True,
            ),
        )


def create_aiven_client() -> AivenClient:
    url = os.getenv("AIVEN_WEB_URL")
    creds_file = os.getenv("AIVEN_CREDENTIALS_FILE")
    if not url or not creds_file:
        raise RuntimeError("AIVEN_WEB_URL and AIVEN_CREDENTIALS_FILE must be set")
    with open(creds_file, "r") as file:
        auth_token = json.load(file)["auth_token"]
    client = AivenClient(base_url=url)
    client.set_auth_token(auth_token)
    return client


@contextmanager
def create_services(client: AivenClient, config: Config) -> Iterator[Iterator[Service]]:
    service_names = []
    try:
        if config.recreate_services:
            terminate_services(client, config.plans, config.project)
            for plan in config.plans:
                service_names.append(create_service(client, plan, config))
        else:
            for plan in config.plans:
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
        return str(client.get_service(config.project, service_name)["service_name"])
    except Error as e:
        if e.status != 404:
            raise

        return create_service(client, plan, config)


def create_service(client: AivenClient, plan: str, config: Config) -> str:
    LOG.info("Creating service %s", plan)
    return str(
        client.create_service(
            cloud=config.cloud,
            service_type="clickhouse",
            plan=plan,
            project=config.project,
            service=plan,
        )["service_name"]
    )


def wait_for_services_to_become_running(
    client: AivenClient, service_names: list[str], project: str, timeout: float = 7200.0
) -> Iterator[ServiceDict]:
    start_time = time()
    while time() - start_time < timeout:
        remaining_services = []
        for service_name in service_names:
            service = client.get_service(project, service_name)
            if service["state"] == "RUNNING":
                if ping(service["service_uri"]):
                    LOG.info("Service %s is reachable", service_name)
                    yield service
                else:
                    remaining_services.append(service_name)
                    LOG.info("Service %s is not reachable", service_name)
            else:
                remaining_services.append(service_name)
                LOG.info("Service %s is %s", service_name, service["state"])
        service_names = remaining_services
        if not service_names:
            return
        sleep(5)
    LOG.warning("Timeout waiting for services to become running: %s", service_names)


def ping(uri: str) -> bool:
    host, port = uri.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=5):
            return True
    except OSError:
        return False


def terminate_services(
    client: AivenClient, service_names: list[str], project: str
) -> None:
    for service_name in service_names:
        terminate_service(client, service_name, project)


def terminate_service(client: AivenClient, service_name: str, project: str) -> None:
    LOG.info("Terminating service %s", service_name)
    try:
        client.delete_service(project, service_name)
    except Error as e:
        if e.status != 404:
            raise
