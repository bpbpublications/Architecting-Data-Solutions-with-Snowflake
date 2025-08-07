/* storage usage query */

 select date_trunc(month, usage_date) as storage_usage_month,
avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as billable_storage_tb
from snowflake.account_usage.storage_usage
group by 1
order by 1;

select 
WAREHOUSE_NAME, 
count(*) as total_number_of_queries
from 
snowflake.account_usage.query_history
where start_time >= date_trunc(month, current_date)
group by warehouse_name;

/* get number of jobs*/

select 
count(*) as total_number_of_jobs
from 
snowflake.account_usage.query_history
where start_time >= date_trunc(month, current_date);

/* get query history */

select
warehouse_name,
sum(credits_used) as total_credits_used
from snowflake.account_usage.warehouse_metering_history
where start_time >= date_trunc(month, current_date)
group by 1, order by 2 desc;

/* warehouse usage */

select 
warehouse_name,
sum(credits_used) as total_credits_used
from 
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
group by warehouse_name;

/*Identify Query Spillage */

SELECT 
query_id, 
substr(query_text,1,50) as partial_query,
bytes_spilled_to_local_storage as query_spilled_bytes, 
bytes_spilled_to_remote_storage as query_storage_bytes,
user_name, 
warehouse_name
FROM
snowflake.account_usage.query_history 
WHERE (bytes_spilled_to_local_storage > 0
OR bytes_spilled_to_remote_storage > 0 )
ORDER BY 
bytes_spilled_to_remote_storage, bytes_spilled_to_local_storage DESC
LIMIT 10; --sample limit to the rows


/*Check Cache utilization */

SELECT 
warehouse_name
,SUM(bytes_scanned) AS Total_bytes_scanned
,SUM(bytes_scanned*percentage_scanned_from_cache) / SUM(bytes_scanned) AS bytes_scanned_from_cache
,SUM(bytes_scanned*percentage_scanned_from_cache) AS bytes_from_cache
,COUNT(*) AS count
FROM 
snowflake.account_usage.query_history
WHERE 
bytes_scanned > 0
GROUP BY 1
ORDER BY 3 DESC;


/*Identifying queries eligible for query acceleration */


SELECT 
query_id, 
eligible_query_acceleration_time
FROM 
SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
where warehouse_name='DEMO_POC_WH'
ORDER BY eligible_query_acceleration_time DESC;


/*Identifying warehouses eligible for query acceleration */


SELECT 
warehouse_name, 
SUM(eligible_query_acceleration_time) AS total_eligible_time
FROM 
SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
GROUP BY warehouse_name
ORDER BY total_eligible_time DESC;

/*Enable query acceleration service for warehouses*/


Create warehouse DEMO_POC_WH with 
ENABLE_QUERY_ACCELERATION = TRUE;

ALTER WAREHOUSE DEMO_POC_WH
SET ENABLE_QUERY_ACCELERATION = true
QUERY_ACCELERATION_MAX_SCALE_FACTOR = 0;

/*Get usage of query acceleration */


SELECT 
query_id,
query_acceleration_bytes_scanned as bytes_scanned,
query_acceleration_upper_limit_scale_factor as scale_factor,
query_acceleration_partitions_scanned as partition_scanned
FROM 
SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE 
query_acceleration_partitions_scanned > 0 
ORDER BY query_acceleration_bytes_scanned DESC;


/*Get billing for query acceleration */


SELECT 
warehouse_name,
SUM(credits_used) AS total_credits_used
FROM 
SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY
WHERE start_time >= DATE_TRUNC(month, CURRENT_DATE)
GROUP BY 1
ORDER BY 2 DESC;


/*Create materialized views */ 

CREATE MATERIALIZED VIEW demo_mv
COMMENT='Demo view'
AS
SELECT 
Set of columns, 
Transformed columns,
Aggregated columns
FROM
Base_table;

/*Create dynamic tables */

CREATE OR REPLACE DYNAMIC TABLE demo_dynamic_table
TARGET_LAG = '30 minutes'
WAREHOUSE = demo_poc_wh
REFRESH_MODE = auto
INITIALIZE = on_create
AS
SELECT 
*
FROM table A;
