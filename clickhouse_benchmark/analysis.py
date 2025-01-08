# polars isn't overkill, what are you talking about?
# Methodology https://github.com/ClickHouse/ClickBench?tab=readme-ov-file#results-usage-and-scoreboards
import importlib.resources as pkg_resources

import altair
import polars as pl

EBS_MONTHLY_COST = 80.0
HYPERDISK_MONTHLY_COST = 80.0
GOOGLE_ATTACHED_SSD_MONTHLY_COST = 88.0
AWS_ATTACHED_SSD_MONTHLY_COST = 0.0

AWS_PLAN_INSTANCE_LOOKUP = {
    "internal-arm-block-storage-16": "m8g.xlarge",
    "internal-arm-block-storage-32": "m8g.2xlarge",
    "internal-arm-block-storage-64": "m8g.4xlarge",
    "internal-arm-block-storage-128": "m8g.8xlarge",
    "internal-block-storage-16": "m7i.xlarge",
    "internal-block-storage-32": "m7i.2xlarge",
    "internal-block-storage-64": "m7i.4xlarge",
    "internal-block-storage-128": "m7i.8xlarge",
    "internal-storage-optimized-16": "i7ie.large",
    "internal-storage-optimized-32": "i7ie.xlarge",
    "internal-storage-optimized-64": "i7ie.2xlarge",
    "internal-arm-storage-optimized-16": "i8g.large",
    "internal-arm-storage-optimized-32": "i8g.xlarge",
    "internal-arm-storage-optimized-64": "i8g.2xlarge",
    "business-16": "i3en.large",
    "business-32": "i3en.xlarge",
    "business-64": "i3en.2xlarge",
}


GOOGLE_PLAN_INSTANCE_LOOKUP = {
    "internal-c3-plans-16": "c3-standard-4-lssd",
    "internal-c3-plans-32": "c3-standard-8-lssd",
    "internal-c3-plans-64": "c3-standard-22-lssd",
    "internal-c4-plans-8": "c4-standard-4",
    "internal-c4-plans-16": "c4-standard-8",
    "internal-c4-plans-32": "c4-standard-16",
    "internal-c4-plans-64": "c4-standard-32",
    "internal-n4-plans-8": "n4-standard-4",
    "internal-n4-plans-16": "n4-standard-8",
    "internal-n4-plans-32": "n4-standard-16",
    "internal-n4-plans-64": "n4-standard-32",
    "business-16": "n2d-highmem-2",
    "business-32": "n2d-highmem-4",
    "business-64": "n2d-highmem-8",
    "internal-business-standard-mem-16": "n2-standard-4",
    "internal-business-standard-mem-32": "n2-standard-8",
    "internal-business-standard-mem-64": "n2-standard-16",
    "internal-c3d-plans-32": "c3d-standard-8-lssd",
    "internal-c3d-plans-64": "c3d-standard-16-lssd",
}

PLAN_LOOKUP_SCHEMA = pl.Schema(
    {
        "plan": pl.String,
        "instance": pl.String,
    }
)

SCHEMA = pl.Schema(
    {
        "plan": pl.String,
        "query": pl.String,
        "hot_total_queries": pl.Int64,
        "hot_succeeded": pl.Int64,
        "hot_failed": pl.Int64,
        "hot_query_duration_ms_0.5": pl.Float64,
        "hot_query_duration_ms_0.9": pl.Float64,
        "hot_memory_usage_0.5": pl.Float64,
        "hot_memory_usage_0.9": pl.Float64,
        "cold_total_queries": pl.Int64,
        "cold_succeeded": pl.Int64,
        "cold_failed": pl.Int64,
        "cold_query_duration_ms_0.5": pl.Float64,
        "cold_query_duration_ms_0.9": pl.Float64,
        "cold_memory_usage_0.5": pl.Float64,
        "cold_memory_usage_0.9": pl.Float64,
    }
)


PRICING_SCHEMA = pl.Schema(
    {
        "instance": pl.String,
        "memory_gb": pl.Float64,
        "vcpus": pl.Int64,
        "price_usd": pl.Float64,
        "ssd_storage": pl.Boolean,
    }
)


def pricing_df(cloud_provider: str) -> pl.LazyFrame:
    if cloud_provider == "aws":
        plan_instance_lookup = AWS_PLAN_INSTANCE_LOOKUP
    elif cloud_provider == "google":
        plan_instance_lookup = GOOGLE_PLAN_INSTANCE_LOOKUP
    plan_df = (
        pl.DataFrame(
            list(plan_instance_lookup.items()),
            schema=PLAN_LOOKUP_SCHEMA,
            orient="row",
        )
        .lazy()
        .with_columns(pl.col("instance").str.extract(r"(\w+)[\.-]").alias("family"))
    )
    with pkg_resources.open_binary(
        "clickhouse_benchmark.pricing", f"{cloud_provider}.csv"
    ) as f:
        return (
            pl.read_csv(f, schema=PRICING_SCHEMA)
            .lazy()
            .join(
                plan_df,
                on="instance",
            )
        )


def results_df() -> pl.LazyFrame:
    df = pl.read_csv("results.csv", schema=SCHEMA).lazy()
    return (
        df.join(
            df.group_by("query").agg(
                pl.min("hot_query_duration_ms_0.5").alias(
                    "baseline_hot_query_duration_ms_0.5"
                ),
                pl.min("hot_query_duration_ms_0.9").alias(
                    "baseline_hot_query_duration_ms_0.9"
                ),
                pl.min("cold_query_duration_ms_0.5").alias(
                    "baseline_cold_query_duration_ms_0.5"
                ),
                pl.min("cold_query_duration_ms_0.9").alias(
                    "baseline_cold_query_duration_ms_0.9"
                ),
            ),
            on="query",
        )
        .select(
            "plan",
            "query",
            ratio(
                pl.col("hot_query_duration_ms_0.5"),
                pl.col("baseline_hot_query_duration_ms_0.5"),
            ).alias("hot_query_duration_ms_0.5_normalized"),
            ratio(
                pl.col("hot_query_duration_ms_0.9"),
                pl.col("baseline_hot_query_duration_ms_0.9"),
            ).alias("hot_query_duration_ms_0.9_normalized"),
            ratio(
                pl.col("cold_query_duration_ms_0.5"),
                pl.col("baseline_cold_query_duration_ms_0.5"),
            ).alias("cold_query_duration_ms_0.5_normalized"),
            ratio(
                pl.col("cold_query_duration_ms_0.9"),
                pl.col("baseline_cold_query_duration_ms_0.9"),
            ).alias("cold_query_duration_ms_0.9_normalized"),
        )
        .group_by("plan")
        .agg(
            geometric_mean(pl.col("hot_query_duration_ms_0.5_normalized")).alias(
                "hot_query_duration_ms_0.5_normalized"
            ),
            geometric_mean(pl.col("hot_query_duration_ms_0.9_normalized")).alias(
                "hot_query_duration_ms_0.9_normalized"
            ),
            geometric_mean(pl.col("cold_query_duration_ms_0.5_normalized")).alias(
                "cold_query_duration_ms_0.5_normalized"
            ),
            geometric_mean(pl.col("cold_query_duration_ms_0.9_normalized")).alias(
                "cold_query_duration_ms_0.9_normalized"
            ),
        )
    ).sort("hot_query_duration_ms_0.5_normalized")


def price_performance_df(
    pricing_df: pl.LazyFrame, results_df: pl.LazyFrame, cloud_provider: str
) -> pl.LazyFrame:
    if cloud_provider == "aws":
        network_storage_cost = EBS_MONTHLY_COST
        ssd_cost = AWS_ATTACHED_SSD_MONTHLY_COST
    elif cloud_provider == "google":
        network_storage_cost = 0.0
        ssd_cost = GOOGLE_ATTACHED_SSD_MONTHLY_COST

    return pricing_df.join(results_df, on="plan").select(
        "plan",
        "instance",
        "family",
        (
            pl.col("price_usd")
            + (
                pl.when(pl.col("ssd_storage"))
                .then(network_storage_cost)
                .otherwise(ssd_cost)
            )
        ).alias("price_usd"),
        pl.col("hot_query_duration_ms_0.5_normalized"),
        pl.col("cold_query_duration_ms_0.5_normalized"),
    )


def plot_price_performance(df: pl.DataFrame) -> None:
    charts = []
    for col in (
        "hot_query_duration_ms_0.5_normalized",
        "cold_query_duration_ms_0.5_normalized",
    ):
        # altair doesn't like column names with dots
        mapped_col = col.replace("_ms_0.5", "")
        chart = (
            df.select(
                "plan",
                "instance",
                "family",
                "price_usd",
                pl.col(col).alias(mapped_col),
            )
            .plot.point(
                x="price_usd",
                y=mapped_col,
                color="family",
                tooltip=["plan", "instance", "price_usd", mapped_col],
            )
            .properties(title=mapped_col, width=800, height=600)
            .interactive()
        )
        charts.append(chart)
    altair.hconcat(*charts).save("price_performance.html")


def perform_analysis(cloud_provider: str) -> None:
    df = results_df()
    p_df = pricing_df(cloud_provider)
    pp_df = price_performance_df(p_df, df, cloud_provider).cache()
    plot_price_performance(pp_df.collect())
    pp_df.collect().write_csv("results_normalized.csv", include_header=True)


INSERT_SCHEMA = pl.Schema(
    {
        "plan": pl.String,
        "total_queries": pl.Int64,
        "succeeded": pl.Int64,
        "failed": pl.Int64,
        "query_duration_ms_0.5": pl.Float64,
        "query_duration_ms_0.9": pl.Float64,
        "memory_usage_ms_0.5": pl.Float64,
        "memory_usage_ms_0.9": pl.Float64,
    }
)


def perform_insert_analysis() -> None:
    df = pl.read_csv("results.csv", schema=INSERT_SCHEMA).lazy()
    df.select(
        pl.col("plan"),
        pl.col("query_duration_ms_0.5"),
        pl.col("query_duration_ms_0.9"),
        pl.min("query_duration_ms_0.5").alias("baseline_query_duration_ms_0.5"),
        pl.min("query_duration_ms_0.9").alias("baseline_query_duration_ms_0.9"),
    ).select(
        "plan",
        ratio(
            pl.col("query_duration_ms_0.5"),
            pl.col("baseline_query_duration_ms_0.5"),
        ).alias("query_duration_ms_0.5_normalized"),
        ratio(
            pl.col("query_duration_ms_0.9"),
            pl.col("baseline_query_duration_ms_0.9"),
        ).alias("query_duration_ms_0.9_normalized"),
    ).sort("query_duration_ms_0.5_normalized").collect().write_csv(
        "results_normalized.csv", include_header=True
    )


def geometric_mean(col: pl.Expr) -> pl.Expr:
    return col.product() ** (1 / col.count())


def ratio(duration: pl.Expr, baseline) -> pl.Expr:
    return (duration + 10) / (baseline + 10)
