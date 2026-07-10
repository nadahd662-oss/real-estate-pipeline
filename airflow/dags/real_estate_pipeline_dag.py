# type: ignore

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule



# FAILURE ALERT FUNCTION


def alert_on_failure(context):
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    logical_date = context.get("logical_date")
    error = context.get("exception")

    print("======================================")
    print("REAL ESTATE PIPELINE FAILURE ALERT")
    print(f"Failed task: {task_id}")
    print(f"Logical date: {logical_date}")
    print(f"Error: {error}")
    print("Please check Airflow logs for more details.")
    print("======================================")



# DEFAULT DAG CONFIGURATION


default_args = {
    "owner": "real_estate_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}



# PROJECT DIRECTORY RESOLUTION


RESOLVE_PROJECT_DIR = r"""
set -e

if [ -n "${PROJECT_DIR:-}" ]; then
    export PROJECT_DIR="$PROJECT_DIR"
elif [ -f "/opt/airflow/project/bronze/load_bronze.py" ]; then
    export PROJECT_DIR="/opt/airflow/project"
elif [ -f "/opt/airflow/bronze/load_bronze.py" ]; then
    export PROJECT_DIR="/opt/airflow"
else
    echo "ERROR: Could not find the project root."
    echo "Expected to find:"
    echo "  /opt/airflow/project/bronze/load_bronze.py"
    echo "or"
    echo "  /opt/airflow/bronze/load_bronze.py"
    exit 1
fi

echo "Using PROJECT_DIR=$PROJECT_DIR"
"""



# DAG DEFINITION


with DAG(
    dag_id="real_estate_medallion_pipeline",
    description="Airflow DAG for Real Estate Data Warehouse: Bronze, Silver, Gold",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["real_estate", "snowflake", "dbt", "medallion"],
) as dag:

    
    # 1: START PIPELINE
    

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    
    # 2: CHECK PROJECT STRUCTURE
    

    check_project_structure = BashOperator(
        task_id="check_project_structure",
        bash_command=RESOLVE_PROJECT_DIR + r"""
        echo "Checking project structure..."

        test -f "$PROJECT_DIR/bronze/load_bronze.py"
        test -d "$PROJECT_DIR/dbt_project/real_estate_dw"
        test -d "$PROJECT_DIR/dbt_project/real_estate_dw/models/silver"
        test -d "$PROJECT_DIR/dbt_project/real_estate_dw/models/gold"

        echo "Project structure is valid."
        """,
    )

    
    # 3: LOAD CSV TO BRONZE LAYER IN SNOWFLAKE
    

    load_bronze = BashOperator(
        task_id="load_csv_to_bronze_snowflake",
        bash_command=RESOLVE_PROJECT_DIR + r"""
        echo "Starting Bronze layer loading..."
        cd "$PROJECT_DIR"
        python bronze/load_bronze.py
        echo "Bronze layer loaded successfully."
        """,
    )

    
    # 4: RUN DBT SILVER MODELS
    

    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver_models",
        bash_command=RESOLVE_PROJECT_DIR + r"""
        echo "Running dbt Silver models..."
        cd "$PROJECT_DIR/dbt_project/real_estate_dw"
        dbt run --select path:models/silver
        echo "dbt Silver models executed successfully."
        """,
    )

    
    # 5: TEST DBT SILVER MODELS
    

    test_dbt_silver = BashOperator(
        task_id="test_dbt_silver_models",
        bash_command=RESOLVE_PROJECT_DIR + r"""
        echo "Running dbt tests for Silver layer..."
        cd "$PROJECT_DIR/dbt_project/real_estate_dw"
        dbt test --select path:models/silver
        echo "dbt Silver tests completed successfully."
        """,
    )

    
    # 6: RUN DBT GOLD MODELS
    

    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold_models",
        bash_command=RESOLVE_PROJECT_DIR + r"""
        echo "Running dbt Gold models..."
        cd "$PROJECT_DIR/dbt_project/real_estate_dw"
        dbt run --select path:models/gold
        echo "dbt Gold models executed successfully."
        """,
    )

    
    # 7: TEST DBT GOLD MODELS
    
    test_dbt_gold = BashOperator(
        task_id="test_dbt_gold_models",
        bash_command=RESOLVE_PROJECT_DIR + r"""
        echo "Running dbt tests for Gold layer..."
        cd "$PROJECT_DIR/dbt_project/real_estate_dw"
        dbt test --select path:models/gold
        echo "dbt Gold tests completed successfully."
        """,
    )

    
    # 8: SUCCESS NOTIFICATION
    

    notify_success = BashOperator(
        task_id="notify_pipeline_success",
        bash_command="""
        echo "======================================"
        echo "REAL ESTATE PIPELINE COMPLETED SUCCESSFULLY"
        echo "Bronze, Silver and Gold layers are updated."
        echo "The Gold layer is ready for Power BI."
        echo "======================================"
        """,
    )

    
    # 9: FAILURE NOTIFICATION
    

    notify_failure = BashOperator(
        task_id="notify_pipeline_failure",
        bash_command="""
        echo "======================================"
        echo "REAL ESTATE PIPELINE FAILED"
        echo "Please check the failed task logs in Airflow."
        echo "======================================"
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    
    # 10: END PIPELINE
    

    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    
    # DEPENDENCIES: MAIN PIPELINE ORDER
    

    (
        start_pipeline
        >> check_project_structure
        >> load_bronze
        >> run_dbt_silver
        >> test_dbt_silver
        >> run_dbt_gold
        >> test_dbt_gold
        >> notify_success
        >> end_pipeline
    )

    
    # DEPENDENCIES: FAILURE PATH
    

    [
        check_project_structure,
        load_bronze,
        run_dbt_silver,
        test_dbt_silver,
        run_dbt_gold,
        test_dbt_gold,
    ] >> notify_failure
