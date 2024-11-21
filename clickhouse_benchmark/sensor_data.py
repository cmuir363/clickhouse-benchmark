from clickhouse_benchmark.config import Config
from clickhouse_benchmark.generate_historical_data import generate_data
from clickhouse_benchmark.generate_tables import create_tables
from clickhouse_benchmark.metadata_clickhouse_loader import generate_metadata
from clickhouse_benchmark.service_matrix import Service

NUM_ROWS_IN_DATASET = 100_000_000


def setup(service: Service, config: Config) -> None:
    create_tables(service.client)
    generate_metadata(service.client)
    generate_data(service.client, NUM_ROWS_IN_DATASET)
