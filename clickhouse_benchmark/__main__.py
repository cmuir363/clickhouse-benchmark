import logging

from clickhouse_benchmark.cli import CLI

LOG = logging.getLogger(__name__)
MAX_CONCURRENT_BENCHMARKS = 10


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    CLI()


if __name__ == "__main__":
    main()
