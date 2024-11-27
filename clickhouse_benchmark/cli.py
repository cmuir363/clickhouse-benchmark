import logging
from collections.abc import Iterator
from concurrent import futures

from typer import Typer

from clickhouse_benchmark.analysis import perform_analysis
from clickhouse_benchmark.benchmark import run_selects
from clickhouse_benchmark.clickbench import setup as clickbench_setup
from clickhouse_benchmark.clickbench_insert import run_insert
from clickhouse_benchmark.config import Config
from clickhouse_benchmark.results import (
    BenchmarkResult,
    write_insert_results_to_csv,
    write_results_to_csv,
)
from clickhouse_benchmark.sensor_data import setup as sensor_data_setup
from clickhouse_benchmark.service_matrix import (
    Service,
    create_aiven_client,
    create_services,
    terminate_services,
)

LOG = logging.getLogger(__name__)
MAX_CONCURRENT_BENCHMARKS = 10

CLI = Typer()


@CLI.command()
def benchmark() -> None:
    config = Config()
    client = create_aiven_client()
    with create_services(client, config) as services:
        with futures.ThreadPoolExecutor(
            max_workers=MAX_CONCURRENT_BENCHMARKS
        ) as executor:
            benchmark_result_futures: list[futures.Future[BenchmarkResult]] = []
            for service in services:
                benchmark_result_futures.append(
                    executor.submit(run_benchmark, service, config)
                )
            write_results_to_csv(
                iterate_result_futures(benchmark_result_futures),
                config.output_file,
            )
    LOG.info("Benchmark finished")


@CLI.command()
def terminate_instances() -> None:
    config = Config()
    client = create_aiven_client()
    terminate_services(client, config.plans, config.project)


@CLI.command()
def analyze_results() -> None:
    perform_analysis(
        [
            "hot_query_duration_ms_0.5",
            "hot_query_duration_ms_0.9",
            "cold_query_duration_ms_0.5",
            "cold_query_duration_ms_0.9",
        ]
    )


@CLI.command()
def analyze_insert_results() -> None:
    perform_analysis(["query_duration_ms_0.5", "query_duration_ms_0.9"])


@CLI.command()
def insert_benchmark() -> None:
    config = Config()
    client = create_aiven_client()
    with create_services(client, config) as services:
        results = []
        for service in services:
            results.append(run_insert(service, config))
        write_insert_results_to_csv(
            results,
            config.output_file,
        )
    LOG.info("Benchmark finished")


def iterate_result_futures(
    benchmark_result_futures: list[futures.Future[BenchmarkResult]],
) -> Iterator[BenchmarkResult]:
    for f in futures.as_completed(benchmark_result_futures):
        if exception := f.exception():
            LOG.error("Error running benchmark", exc_info=exception)
        else:
            yield f.result()


def run_benchmark(service: Service, config: Config) -> BenchmarkResult:
    if config.test == "clickbench":
        clickbench_setup(service, config)
    elif config.test == "sensor_data":
        sensor_data_setup(service, config)
    return run_selects(service, config)
