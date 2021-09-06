
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = 'snowflake'
SLACK_CONN_ID = 'slack'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'ADVENTUREWORKS'
SNOWFLAKE_ROLE = 'SYSADMIN'

# UPDATE TABLE
CORRELATION_CATEGORY_SQL= ( 
    "create or replace table public.correlations_by_category as "+
    "WITH product_sales AS (SELECT p.productid,productsubcategoryid,p.name,OrderQty,p.listprice,s.unitprice FROM "+
                        "product p "+
                        "JOIN salesorderdetail s ON (p.productid = s.productid)), "+
        "top_subcategories AS ( SELECT productsubcategoryid, SUM(OrderQty) AS totalSales "+
                            "FROM product_sales "+
                            "GROUP BY productsubcategoryid "+
                            "ORDER BY totalSales DESC "+
                            "LIMIT 10), "+
        "products_topcategories AS (SELECT productsubcategoryid,productid,name, OrderQty,unitprice "+
                            "FROM product_sales "+
                            "WHERE productsubcategoryid IN (SELECT productsubcategoryid FROM top_subcategories) "+
                                ") "+
    "SELECT productsubcategoryid, productid,name,corr(OrderQty, unitprice) as corr FROM  "+
    "products_topcategories "+
    "GROUP BY (productsubcategoryid, productid,name) "+
    "ORDER BY corr ASC "+
    "LIMIT 10;" 
)
# INSERT
CORRELATION_SUBT_COMISSION_SQL = (
    "insert into public.correlation_subtotal_comission_currency_ok (correlation,date) "+
    "with sales_rate_currency as (SELECT s.orderdate,s.salespersonid,s.currencyrateid, s.subtotal,c.averagerate FROM "+
                        "currencyrate c join "+
                        "salesorderheader s on c.currencyrateid = s.currencyrateid), "+
        "salesorder_currency AS (SELECT s.salespersonid,s.currencyrateid, s.subtotal, "+
                                    "(CASE "+
                                    "WHEN s.currencyrateid IS NULL THEN 1 "+
                                    "ELSE s.averagerate "+
                                    "END) as averagerate "+
                                    "FROM  "+
                                    "sales_rate_currency s "+
                                    "WHERE s.salespersonid IS NOT NULL "+
                                    "AND s.orderdate BETWEEN '2014-01-01'::date  "+
                                    "AND '2014-12-31'::date), "+
        "salesorder_usd AS (SELECT salespersonid,subtotal*averagerate as subtotal_usd "+
                        "FROM  "+
                        "salesorder_currency "+
                        "ORDER BY subtotal_usd DESC) "+
    "SELECT corr(su.subtotal_usd, sp.commissionpct) as correlation,GETDATE() as date FROM  "+
    "salesORDER_usd su "+
    "left JOIN salesperson sp "+
    "ON (su.salespersonid=sp.businessentityid);"
)

#MERGE
CREATE_TEMP_TABLE_SQL=(
    "create or replace table public.temp_top_products as "+
    "WITH top_products_ids AS (SELECT productid, SUM(OrderQty) AS totalSales "+
                        "from salesorderdetail "+
                        "GROUP BY productid "+
                        "ORDER BY totalSales DESC "+
                        "LIMIT 10) "+
    "SELECT p.productmodelid as ModelID, name, description, totalsales,getdate() as date_in_top FROM  "+
    "top_products_ids tp "+
    "LEFT JOIN product p ON (p.productid = tp.productid) "+
    "LEFT JOIN productmodelproductdescriptionculture pm ON (pm.productmodelid = p.productmodelid) "+
    "LEFT JOIN productdescription pd ON (pd.productdescriptionid = pm.productdescriptionid) "+
    "WHERE pm.cultureid like '%en%'"
    "ORDER BY totalSales DESC;"

)
MERGE_TABLE_SQL=(
    "merge into public.top_products as target using public.temp_top_products as source "+
    "on source.ModelID = target.ModelID "+
    "when not matched then "+
        "INSERT (ModelID, name, description, totalsales,date_in_top)  "+
        "VALUES (source.ModelID,source.name,source.description,source. totalsales,source.date_in_top);"
)
DROP_TEMP_TABLE_SQL=(
    "drop table public.temp_top_products;"
)
# SQL commands

# Slack
SNOWFLAKE_SLACK_SQL = "SELECT TABLE_NAME, CREATED, LAST_ALTERED FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = 'PUBLIC';"


with DAG("dags_adventureworks", start_date=days_ago(2),
        schedule_interval="*/2 * * * *",tags=['adventureworks','shop'], catchup=False) as dag:

    update_correlation_category = SnowflakeOperator(
        task_id='update_correlation_category',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CORRELATION_CATEGORY_SQL,
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
    #insert data in table
    add_correlation = SnowflakeOperator(
        task_id='add_correlation',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CORRELATION_SUBT_COMISSION_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    (

        [
            update_correlation_category,

            add_correlation,

            (create_temp_table >> merge_table >> drop_temp_table)
        ]
    )