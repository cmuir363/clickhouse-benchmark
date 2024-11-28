# polars isn't overkill, what are you talking about?
# Methodology https://github.com/ClickHouse/ClickBench?tab=readme-ov-file#results-usage-and-scoreboards
import polars as pl

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


def perform_analysis() -> None:
    df = pl.read_csv("results.csv", schema=SCHEMA).lazy()
    df = (
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
            ).alias("hot_query_duration_ms_0.5_ratio"),
            ratio(
                pl.col("hot_query_duration_ms_0.9"),
                pl.col("baseline_hot_query_duration_ms_0.9"),
            ).alias("hot_query_duration_ms_0.9_ratio"),
            ratio(
                pl.col("cold_query_duration_ms_0.5"),
                pl.col("baseline_cold_query_duration_ms_0.5"),
            ).alias("cold_query_duration_ms_0.5_ratio"),
            ratio(
                pl.col("cold_query_duration_ms_0.9"),
                pl.col("baseline_cold_query_duration_ms_0.9"),
            ).alias("cold_query_duration_ms_0.9_ratio"),
        )
        .group_by("plan")
        .agg(
            geometric_mean(pl.col("hot_query_duration_ms_0.5_ratio")).alias(
                "hot_query_duration_ms_0.5_result"
            ),
            geometric_mean(pl.col("hot_query_duration_ms_0.9_ratio")).alias(
                "hot_query_duration_ms_0.9_result"
            ),
            geometric_mean(pl.col("cold_query_duration_ms_0.5_ratio")).alias(
                "cold_query_duration_ms_0.5_result"
            ),
            geometric_mean(pl.col("cold_query_duration_ms_0.9_ratio")).alias(
                "cold_query_duration_ms_0.9_result"
            ),
        )
    ).sort("hot_query_duration_ms_0.5_result")
    # write results
    df.collect().write_csv("results_ratio.csv", include_header=True)


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
        ).alias("query_duration_ms_0.5_ratio"),
        ratio(
            pl.col("query_duration_ms_0.9"),
            pl.col("baseline_query_duration_ms_0.9"),
        ).alias("query_duration_ms_0.9_ratio"),
    ).sort("query_duration_ms_0.5_ratio").collect().write_csv(
        "results_ratio.csv", include_header=True
    )


def geometric_mean(col: pl.Expr) -> pl.Expr:
    return col.product() ** (1 / col.count())


def ratio(duration: pl.Expr, baseline) -> pl.Expr:
    return (duration + 10) / (baseline + 10)
