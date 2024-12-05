import csv
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any

from pydantic import BaseModel

from clickhouse_benchmark.client import ClickHouseClient

QUANTILES = [0.5, 0.9]

LOG = logging.getLogger(__name__)


class QueryStatistics(BaseModel):
    query: str
    total_queries: int
    succeeded: int
    failed: int
    quantiles: list[float]
    query_duration_ms: list[float]
    memory_usage: list[float]


class QueryResult(BaseModel):
    query: str
    hot: QueryStatistics
    cold: QueryStatistics


class BenchmarkResult(BaseModel):
    plan: str
    results: list[QueryResult]


class InsertBenchmarkResult(BaseModel):
    plan: str
    results: QueryStatistics


def get_query_result(
    client: ClickHouseClient, query: str, hot_ids: list[str], cold_ids: list[str]
) -> QueryResult:
    client.execute_all_nodes("SYSTEM FLUSH LOGS")
    return QueryResult(
        query=query,
        hot=get_query_statistics(client, hot_ids),
        cold=get_query_statistics(client, cold_ids),
    )


def get_query_statistics(
    client: ClickHouseClient, query_ids: list[str]
) -> QueryStatistics:
    quantiles_str = ", ".join(str(q) for q in QUANTILES)
    query = f"""
    SELECT
        any(query) AS query,
        count() AS total_queries,
        countIf(type = 'QueryFinish') AS succeeded,
        countIf(type = 'ExceptionWhileProcessing') AS failed,
        {QUANTILES} as quantiles,
        quantilesIf({quantiles_str})(memory_usage, type = 'QueryFinish') as memory_usage,
        quantilesIf({quantiles_str})(query_duration_ms, type = 'QueryFinish') as query_duration_ms
    FROM
        clusterAllReplicas('default', system.query_log)
    WHERE
        (type = 'QueryFinish' OR type = 'ExceptionWhileProcessing')
        AND query_id IN {query_ids}
    """
    resp = client.execute(query)
    return QueryStatistics(**resp.results[0])


class ResultsWriter:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._file: IO | None = None
        self._writer: Any = None

    def _get_writer(self) -> Any:
        if self._writer is None:
            self._file = open(self.file_path, "w")
            self._writer = csv.writer(self._file)
            self._writer.writerow(
                [
                    "plan",
                    "query",
                    "hot_total_queries",
                    "hot_succeeded",
                    "hot_failed",
                    *["hot_query_duration_ms_" + str(q) for q in QUANTILES],
                    *["hot_memory_usage_ms_" + str(q) for q in QUANTILES],
                    "cold_total_queries",
                    "cold_succeeded",
                    "cold_failed",
                    *["cold_query_duration_ms_" + str(q) for q in QUANTILES],
                    *["cold_memory_usage_ms_" + str(q) for q in QUANTILES],
                ]
            )
        return self._writer

    def close(self):
        if self._file is not None:
            self._file.close()

    def write(self, result: BenchmarkResult) -> None:
        LOG.info("Writing results for %s", result.plan)
        for query_result in result.results:
            self._get_writer().writerow(
                [
                    result.plan,
                    query_result.query.strip(),
                    query_result.hot.total_queries,
                    query_result.hot.succeeded,
                    query_result.hot.failed,
                    *query_result.hot.query_duration_ms,
                    *query_result.hot.memory_usage,
                    query_result.cold.total_queries,
                    query_result.cold.succeeded,
                    query_result.cold.failed,
                    *query_result.cold.query_duration_ms,
                    *query_result.cold.memory_usage,
                ]
            )


def write_insert_results_to_csv(
    results: Iterable[InsertBenchmarkResult], file_path: Path
) -> None:
    with open(file_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "plan",
                "total_queries",
                "succeeded",
                "failed",
                *["query_duration_ms_" + str(q) for q in QUANTILES],
                *["memory_usage_ms_" + str(q) for q in QUANTILES],
            ]
        )
        for result in results:
            LOG.info("Writing results for %s", result.plan)
            writer.writerow(
                [
                    result.plan,
                    result.results.total_queries,
                    result.results.succeeded,
                    result.results.failed,
                    *result.results.query_duration_ms,
                    *result.results.memory_usage,
                ]
            )
