from pathlib import Path

from pydantic import BaseModel


class Config(BaseModel):
    query_file: Path = Path("clickhouse_benchmark/queries/analytical-queries.sql")
    query_run_count: int = 10
    output_file: Path = Path("results.csv")
    num_rows_in_dataset: int = 10_000_000
    terminate_services_on_exit: bool = True
    plans: list[str] = [
        "business-16",
        "business-32",
        "business-64",
        "internal-storage-optimized-16",
        "internal-storage-optimized-32",
        "internal-storage-optimized-64",
        "internal-arm-storage-optimized-16",
        "internal-arm-storage-optimized-32",
        "internal-arm-storage-optimized-64",
        "internal-block-storage-8",
        "internal-block-storage-16",
        "internal-block-storage-32",
        "internal-block-storage-64",
        "internal-block-storage-128",
        "internal-arm-block-storage-8",
        "internal-arm-block-storage-16",
        "internal-arm-block-storage-32",
        "internal-arm-block-storage-64",
        "internal-arm-block-storage-128",
    ]
    cloud: str = "aws-us-west-2"
    project: str = "test"
