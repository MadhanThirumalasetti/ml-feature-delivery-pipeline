"""
ml_feature_pipeline_dag.py

Daily multi-cloud ML feature delivery pipeline.
Ingests Salesforce CRM data, runs PySpark feature engineering
on Databricks, writes to Redshift + BigQuery simultaneously,
then runs dbt models for final ML-ready feature tables.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["tmadhan0063@gmail.com"],
}


with DAG(
    dag_id="ml_feature_delivery_pipeline",
    description="Daily Salesforce CRM → PySpark features → Redshift + BigQuery",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 4 * * *",  # 4 AM UTC daily
    catchup=False,
    tags=["ml", "features", "databricks", "multi-cloud"],
) as dag:

    # Task 1: Run PySpark feature engineering on Databricks
    spark_feature_job = DatabricksSubmitRunOperator(
        task_id="run_spark_feature_engineering",
        databricks_conn_id="databricks_default",
        existing_cluster_id="{{ var.value.databricks_cluster_id }}",
        spark_python_task={
            "python_file": "dbfs:/jobs/feature_engineering.py",
        },
    )

    # Task 2: Run dbt feature models
    dbt_run = BashOperator(
        task_id="dbt_run_feature_models",
        bash_command=(
            "cd /opt/dbt && "
            "dbt run --select mart.fct_account_features "
            "--target prod --profiles-dir /opt/dbt/profiles"
        ),
    )

    # Task 3: Validate feature table quality
    def validate_features(**context):
        import great_expectations as ge

        context_ge = ge.get_context()
        result = context_ge.run_checkpoint(
            checkpoint_name="account_features_checkpoint"
        )
        if not result["success"]:
            raise ValueError("Feature table validation failed — blocking model consumption")

        # Push record count to XCom for monitoring
        stats = result["run_results"]
        context["ti"].xcom_push(key="validation_result", value=str(stats))

    validate_task = PythonOperator(
        task_id="validate_feature_quality",
        python_callable=validate_features,
    )

    # Task 4: dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test_feature_models",
        bash_command=(
            "cd /opt/dbt && "
            "dbt test --select mart.fct_account_features "
            "--target prod --profiles-dir /opt/dbt/profiles"
        ),
    )

    # Pipeline order
    spark_feature_job >> dbt_run >> validate_task >> dbt_test
