
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = 'snowflake'
SLACK_CONN_ID = 'slack'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'MEETUP'
SNOWFLAKE_ROLE = 'SYSADMIN'
SNOWFLAKE_SAMPLE_TABLE = 'aux.correlation_duration_yesrsvp'

# SQL commands
test_sql = (
    """insert into aux.correlation_duration_yesrsvp (correlation,date) select corr(duration,yes_rsvp_count) as correlation,GETDATE() as date  from events;"""
    )

SNOWFLAKE_SLACK_SQL = """SELECT * FROM aux.correlation_duration_yesrsvp;"""
SNOWFLAKE_SLACK_MESSAGE = "The table was updated, :D "



with DAG("test_data2slack", start_date=days_ago(2),
        schedule_interval="*/2 * * * *",tags=['example'], catchup=False) as dag:


    snowflake_TEST = SnowflakeOperator(
        task_id='snowflake_TEST',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=test_sql ,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    slack_report = SnowflakeToSlackOperator(
        task_id="slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message=SNOWFLAKE_SLACK_MESSAGE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        slack_conn_id=SLACK_CONN_ID,
    )


    (
        snowflake_TEST >> slack_report
    )

