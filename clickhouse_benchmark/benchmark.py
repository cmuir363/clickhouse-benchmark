import logging
import re
from pathlib import Path

from clickhouse_connect.driver.client import Client

from clickhouse_benchmark.config import Config
from clickhouse_benchmark.results import BenchmarkResult, get_query_result
from clickhouse_benchmark.service_matrix import Service

LOG = logging.getLogger(__name__)


def run_selects(service: Service, config: Config) -> BenchmarkResult:
    LOG.info("Running selects for %s", service.plan)
    queries = load_query_file(config.query_file)
    result = []
    for query in queries:
        hot_ids = run_query_hot(service.client, query, config.query_run_count)
        cold_ids = run_query_cold(service.client, query, config.query_run_count)
        result.append(get_query_result(service.client, query, hot_ids, cold_ids))
    return BenchmarkResult(
        plan=service.plan,
        results=result,
    )


def load_query_file(file_path: Path) -> list[str]:
    with open(file_path, "r") as file:
        text = file.read()
    # delete comments
    text = re.sub(r"--.*\n", "", text)
    # delete empty lines
    text = re.sub(r"\n+", "\n", text)
    return text.strip(";\n").split(";")


def run_query_hot(client: Client, query: str, count: int) -> list[str]:
    LOG.info("Running hot queries")
    result = []
    # warm caches
    client.query(query, parameters={"format": "Null"})
    for _ in range(count):
        LOG.info("Running hot query: %s", query)
        resp = client.query(query, parameters={"format": "Null"})
        result.append(resp.query_id)
    return result


def run_query_cold(client: Client, query: str, count: int) -> list[str]:
    LOG.info("Running cold queries")
    result = []
    for _ in range(count):
        LOG.info("Running cold query: %s", query)
        resp = client.query(
            query, parameters={"format": "Null", "min_bytes_to_use_direct_io": 1}
        )
        result.append(resp.query_id)
    return result
