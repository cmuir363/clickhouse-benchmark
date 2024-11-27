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


def perform_analysis(cols: list[str]) -> None:
    df = pl.read_csv("results.csv").lazy()
    baseline_cols = [f"baseline_{col}" for col in cols]
    ratio_cols = [f"{col}_ratio" for col in cols]
    result_cols = [f"{col}_result" for col in ratio_cols]

    df = (
        df.join(
            df.group_by("query").agg(
                [
                    pl.min(col).alias(baseline_col)
                    for col, baseline_col in zip(cols, baseline_cols)
                ]
            ),
            on="query",
        )
        .select(
            ["plan", "query"]
            + [
                ratio(pl.col(col), pl.col(baseline_col)).alias(ratio_col)
                for col, baseline_col, ratio_col in zip(cols, baseline_cols, ratio_cols)
            ]
        )
        .group_by("plan")
        .agg(
            [
                geometric_mean(pl.col(ratio_col)).alias(result_col)
                for ratio_col, result_col in zip(ratio_cols, result_cols)
            ]
        )
    ).sort(result_cols[0])
    # write results
    df.collect().write_csv("results_ratio.csv", include_header=True)


def geometric_mean(col: pl.Expr) -> pl.Expr:
    return col.product() ** (1 / col.count())


def ratio(duration: pl.Expr, baseline) -> pl.Expr:
    return (duration + 10) / (baseline + 10)
