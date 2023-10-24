from functools import partial

import pendulum
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airhelp.operators.slack import alert_slack_channel
from pendulum.tz import timezone


def get_environment() -> str:
    """Get the environment."""
    return Variable.get("environment", "staging")


def is_production():
    """Check if the environment is production."""
    return get_environment() == "production"


@dag(
    dag_id="ga360_loader",
    description="Load Google Analytics data to Snowflake",
    start_date=pendulum.yesterday(),
    schedule="0 5 * * *",  # 05:00 UTC
    catchup=False,
    tags=["google-analytics", "ga360", "snowflake", "big-query", "gcs"],
    on_failure_callback=partial(
        alert_slack_channel,
        "ga360",
        "ga_sessions",
        "failed",
        message="Google Analytics 360 sessions haven't been updated due to an error",
    ),
    on_success_callback=partial(
        alert_slack_channel,
        "ga360",
        "ga_sessions",
        "success",
        message="Google Analytics 360 sessions have been updated successfully",
    ),
)
def ga360_loader():
    ga360_loader_days_ago = Variable.get("ga360_loader_days_ago", 7)

    load_intraday = TriggerDagRunOperator(
        task_id=f"load_intraday",
        trigger_dag_id="ga360_loader_intraday",
        execution_date=pendulum.today().subtract(days=1),
        wait_for_completion=True,
        reset_dag_run=True,
    )

    load_yesterday = TriggerDagRunOperator(
        task_id=f"load_yesterday",
        trigger_dag_id="ga360_loader_daily",
        execution_date=pendulum.today().subtract(days=1),
        wait_for_completion=True,
        reset_dag_run=True,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS)

    [load_intraday, load_yesterday] >> end

    for i in range(2, ga360_loader_days_ago + 1):
        load_day = TriggerDagRunOperator(
            task_id=f"load_{i}_days_ago",
            trigger_dag_id="ga360_loader_daily",
            execution_date=pendulum.today().subtract(days=i),
            wait_for_completion=True,
            reset_dag_run=True,
            retries=1,
        )
        load_day >> [load_intraday, load_yesterday]


@task(task_id="get_days_sequence")
def get_days_sequence(**kwargs):
    # Create a sequence of dates from a date to today
    ga360_loader_from_date = kwargs["dag_run"].conf["from_date"]
    start = pendulum.from_format(ga360_loader_from_date, "YYYY-MM-DD")
    period = pendulum.period(start, pendulum.yesterday())
    return [dt.format("YYYY-MM-DD") for dt in period.range("days")]


@task(task_id="ga360_from_date_loader")
def ga360_from_date_loader(day: str, **kwargs):
    # Load data for a specific date
    context = get_current_context()
    TriggerDagRunOperator(
        task_id=f"load_{day}",
        trigger_dag_id="ga360_loader_daily",
        execution_date=day,
        wait_for_completion=False,
        reset_dag_run=True,
    ).execute(context=context)


@dag(
    dag_id="ga360_loader_from_date",
    description="Load Google Analytics data to Snowflake from a specific date",
    start_date=pendulum.yesterday(),
    schedule=None,
    catchup=False,
    tags=["google-analytics", "ga360", "snowflake", "big-query", "gcs"],
    on_failure_callback=partial(
        alert_slack_channel, "ga360", "ga_sessions", "failed"
    ),
)
def ga360_loader_from_date():
    ga360_from_date_loader.expand(day=get_days_sequence())


ga360_loader_dag = ga360_loader()
ga360_loader_from_date_dag = ga360_loader_from_date()
