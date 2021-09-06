
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = 'snowflake'
SLACK_CONN_ID = 'slack'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'MEETUP'
SNOWFLAKE_ROLE = 'SYSADMIN'


# SQL commands
test_sql = (
    """insert into aux.correlation_duration_yesrsvp (correlation,date) select corr(duration,yes_rsvp_count) as correlation,GETDATE() as date  from events;"""
    )
# 1.
MEMBERS_ALL_CITY_SQL = (
    "create or replace table aux.members_all_city as "+
    "with members_city as (select distinct member_id,city "+
    "from public.members),members_city_clean_parts as (select "+
    "replace(replace(replace(replace(replace(city,'san francisco','San Francisco') "+
    ",' Park','') ,' Mills','') ,' Ridge',''),' Heights','') as city,member_id "+
    "from members_city), members_city_clean_cardinality as (select "+
    "replace(replace(replace(replace(city,'East ',''),'West ',''),'North ','') "+
    ",'South ','') as city, member_id from members_city_clean_parts ) "+
    "select city, count(member_id) as members from members_city_clean_cardinality "+
    "group by city order by members asc;"
)
# 2.
AVG_CHICAGO_RATING_SQL = (
    "create or replace table aux.avg_rating_chicago as "+
    "select city, avg(rating) as avg_rating "+
    "from public.groups where state = 'IL' "+
    "group by city order by avg_rating desc; "
)
# 3.
CORRELATION_DUR_YES_SQL = (
    "insert into aux.correlation_duration_yesrsvp (correlation,date) "+
    "select corr(duration,yes_rsvp_count) as correlation, "+
    "GETDATE() as date from public.events;"
)

#4
POPULAR_CATEGORIES_SQL= ( 
    "create or replace table aux.popular_categories as "+
    "select c.category_name, sum(g.members) as total_members "+
    "from groups g "+
    "join categories c on g.category_id = c.category_id "+
    "group by c.category_name "+
    "order by total_members desc;"
)
# 5
GROUPS_GROWTH_SQL = (
    "create or replace table aux.groups_growth as "+
    "with group_year_creation as (select "+
            "group_id, "+
                        "created, "+
                        "extract (year from created) as year, "+
                        "extract (month from created) as month "+
                    "from "+
                        "public.groups), "+
    "groups_by_date as (select year,month, "+
                        "count(group_id) as groups "+
                    "from group_year_creation "+
                    "group by year,month "+
                    "order by year,month) "+
    "select concat(year,'-',month) as month,groups "+
    "from groups_by_date;"
    )
# 6
TECHNO_TOPICS_SQL = (
    "create or replace table aux.technology_topics as "+
    "with groups_by_topic as (select topic_id,  "+
                            "count(group_id) as groups "+
                            "from groups_topics group by topic_id), "+
        "techno_topic_key_ids as (select distinct topic_id "+ 
                                "from members_topics "+
                        "where topic_key like '%Techno%' "+
                            "or topic_key like '%techno%') "+
    "select t.topic_id,t.topic_name,t.members, gt.groups ,t.link "+
    "from public.topics t "+
    "join groups_by_topic gt  "+
    "on t.topic_id = gt.topic_id "+
    "where t.topic_id in (select * from techno_topic_key_ids) "+
    "order by members desc;"
)
#7
CREATE_TEMP_TABLE_SQL=(
    "create or replace table aux.temp_best_rated_venues as "
    "select venue_id, address_1 as address,city, normalised_rating, "  
    "getdate() as date_in_top from venues order by normalised_rating "+
    "desc limit 15;"
)
MERGE_TABLE_SQL=(
    "merge into aux.best_rated_venues as target using aux.temp_best_rated_venues as source "+
    "on source.venue_id = target.venue_id "+
    "when not matched then "+
        "INSERT (venue_id,address, city,normalised_rating,date_in_top) "+
        "VALUES (source.venue_id,source.address,source.city,source.normalised_rating,source.date_in_top);"
)
DROP_TEMP_TABLE_SQL=(
    "drop table aux.temp_best_rated_venues;"
)
# Slack
SNOWFLAKE_SLACK_SQL = "SELECT TABLE_NAME, CREATED, LAST_ALTERED FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = 'AUX';"


with DAG("dags_meetup", start_date=days_ago(2),
        schedule_interval="*/15 * * * *",tags=['meetup','events'], catchup=False) as dag:

    # update tables
    update_members_all_city = SnowflakeOperator(
        task_id='update_members_all_city',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=MEMBERS_ALL_CITY_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )
    update_chicago_ratings = SnowflakeOperator(
        task_id='update_chicago_ratings',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=AVG_CHICAGO_RATING_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    update_popular_categories = SnowflakeOperator(
        task_id='update_popular_categories',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=POPULAR_CATEGORIES_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )
    
    update_groups_growth = SnowflakeOperator(
        task_id='update_groups_growth',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=GROUPS_GROWTH_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )
    
    update_techno_topics = SnowflakeOperator(
        task_id='update_techno_topics',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=TECHNO_TOPICS_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )
    
    #insert data in table
    add_correlation = SnowflakeOperator(
        task_id='add_correlation',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CORRELATION_DUR_YES_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    # merge data in table
    create_temp_table = SnowflakeOperator(
        task_id='create_temp_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_TEMP_TABLE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )
    merge_table = SnowflakeOperator(
        task_id='merge_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=MERGE_TABLE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )
    drop_temp_table = SnowflakeOperator(
        task_id='drop_temp_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=DROP_TEMP_TABLE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )
    
    #slack report
    starting_slack_report = SnowflakeToSlackOperator(
        task_id="starting_slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message="Update proccess started \uD83E\uDD14 \uD83E\uDD1E",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        slack_conn_id=SLACK_CONN_ID,
    )

    update_slack_report = SnowflakeToSlackOperator(
        task_id="update_slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message="The tables in snowflake \u2744 were correctly updated \uD83D\uDE03",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        slack_conn_id=SLACK_CONN_ID,
    )
    insert_slack_report = SnowflakeToSlackOperator(
        task_id="insert_slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message="The data was correctly inserted in snowflake \u2744 tables \uD83D\uDE00 ",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        slack_conn_id=SLACK_CONN_ID,
    )
    merge_slack_report = SnowflakeToSlackOperator(
        task_id="merge_slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message="The data was correctly merged in snowflake \u2744 tables \uD83D\uDE04 ",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        slack_conn_id=SLACK_CONN_ID,
    )
    final_slack_report = SnowflakeToSlackOperator(
        task_id="final_slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message="All the operatins was done correctly in snowflake \u2744 \uD83E\uDD73",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        slack_conn_id=SLACK_CONN_ID,
    )


    (
        starting_slack_report
        >>
        [
            ([
               update_members_all_city,
               update_chicago_ratings,
               update_popular_categories,
               update_groups_growth,
               update_techno_topics
            ]
            >> update_slack_report),

            (add_correlation >> insert_slack_report),

            (create_temp_table >> merge_table >> drop_temp_table >> merge_slack_report)
        ]
        >>
        final_slack_report
        
    )

