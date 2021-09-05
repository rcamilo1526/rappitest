
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
# from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = 'snowflake'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'MEETUP'
SNOWFLAKE_ROLE = 'SYSADMIN'
SNOWFLAKE_SAMPLE_TABLE = 'members_usuarios_ciudad'

# SQL commands
test_sql = """create or replace table aux.members_usuarios_ciudad as select city ,member_id,min(joined) as min_joined,max(joined) as max_joined from public.members m group by 1, 2;"""


with DAG("example_snowflake", start_date=days_ago(2),
        schedule_interval="*/15 * * * *",tags=['example'], catchup=False) as dag:


    snowflake_TEST = SnowflakeOperator(
        task_id='snowflake_TEST',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=test_sql ,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )


    (
        snowflake_TEST
    )

