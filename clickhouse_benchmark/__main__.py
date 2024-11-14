import logging
from concurrent import futures

from clickhouse_benchmark.benchmark import run_selects
from clickhouse_benchmark.config import Config
from clickhouse_benchmark.generate_historical_data import generate_data
from clickhouse_benchmark.generate_tables import create_tables
from clickhouse_benchmark.metadata_clickhouse_loader import generate_metadata
from clickhouse_benchmark.results import BenchmarkResult, write_results_to_csv
from clickhouse_benchmark.service_matrix import (
    Service,
    create_aiven_client,
    create_services,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    config = Config()
    client = create_aiven_client()
    with create_services(client, config) as services:
        with futures.ThreadPoolExecutor(max_workers=10) as executor:
            benchmark_result_futures = []
            for service in services:
                benchmark_result_futures.append(
                    executor.submit(run_benchmark, service, config)
                )
            write_results_to_csv(
                (f.result() for f in futures.as_completed(benchmark_result_futures)),
                config.output_file,
            )


def run_benchmark(service: Service, config: Config) -> BenchmarkResult:
    create_tables(service.client)
    generate_metadata(service.client)
    generate_data(service.client, config.num_rows_in_dataset)
    return run_selects(service, config)


if __name__ == "__main__":
    main()
