"""
feature_engineering.py

PySpark feature engineering job for the ML forecasting pipeline.
Reads raw CRM data from Redshift, computes features, and writes
ML-ready feature tables to both AWS Redshift and GCP BigQuery.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("salesforce-feature-engineering")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


def compute_account_features(df):
    """
    Compute account-level rolling features used by the
    Scikit-learn forecasting model.
    """
    window_90d = Window.partitionBy("account_id").orderBy("close_date").rangeBetween(
        -90 * 86400, 0  # 90 day rolling window in seconds
    )
    window_all = Window.partitionBy("account_id").orderBy("close_date")

    return df.withColumn(
        "rolling_90d_revenue", F.sum("amount_usd").over(window_90d)
    ).withColumn(
        "rolling_90d_deal_count", F.count("opportunity_id").over(window_90d)
    ).withColumn(
        "avg_deal_size", F.avg("amount_usd").over(window_all)
    ).withColumn(
        "days_since_last_close",
        F.datediff(F.current_date(), F.lag("close_date", 1).over(window_all))
    ).withColumn(
        "win_rate",
        F.avg(F.when(F.col("stage_name") == "Closed Won", 1.0).otherwise(0.0)).over(window_all)
    )


def write_to_redshift(df, table: str, redshift_url: str, temp_s3_path: str):
    """Write feature table to AWS Redshift via S3 temp path."""
    (
        df.write
        .format("com.databricks.spark.redshift")
        .option("url", redshift_url)
        .option("dbtable", f"features.{table}")
        .option("tempdir", temp_s3_path)
        .option("aws_iam_role", "arn:aws:iam::ACCOUNT:role/RedshiftS3Role")
        .mode("overwrite")
        .save()
    )


def write_to_bigquery(df, table: str, gcp_project: str, dataset: str):
    """Write feature table to GCP BigQuery."""
    (
        df.write
        .format("bigquery")
        .option("project", gcp_project)
        .option("dataset", dataset)
        .option("table", table)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .save()
    )


def main():
    spark = build_spark_session()

    # Read raw opportunities from Redshift
    raw_df = (
        spark.read
        .format("com.databricks.spark.redshift")
        .option("url", "jdbc:redshift://host:5439/db")
        .option("dbtable", "raw.salesforce_opportunities")
        .option("tempdir", "s3://bucket/temp/")
        .load()
    )

    # Compute features
    features_df = compute_account_features(raw_df)

    # Drop nulls on key feature columns before serving to model
    features_df = features_df.dropna(subset=[
        "rolling_90d_revenue",
        "avg_deal_size",
        "win_rate"
    ])

    # Write to both clouds simultaneously — zero schema drift
    write_to_redshift(features_df, "account_features", "jdbc:redshift://host:5439/db", "s3://bucket/temp/")
    write_to_bigquery(features_df, "account_features", "my-gcp-project", "ml_features")


if __name__ == "__main__":
    main()
