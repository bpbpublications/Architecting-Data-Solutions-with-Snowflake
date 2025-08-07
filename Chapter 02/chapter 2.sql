/* ----------Using Query History -----------*/

/* -- geting details of query history --*/

select * from account_usage.query_history;

/*get queries using cloud services cost */

select * from account_usage.query_history where CREDITS_USED_CLOUD_SERVICES > 0; 

/* getting history of queries ran in last 60 days */

select sum(credits_used) as total_credits, sum(credits_used_compute) as compute_credits, sum(credits_used_cloud_services) as cloud_services_credits  from 
account_usage.warehouse_metering_history where datediff(day,cast(start_time as date),current_date)= 60; 

/* Create warehouse and grant permissions to role */

Use role accountadmin;
Create role dev_adhoc_user;
Create role read_only_user;
Use role sysadmin;
Create warehouse dev_adhoc_wh with warehouse_size='Small' INITIALLY_SUSPENDED=TRUE;
GRANT OPERATE ON WAREHOUSE dev_adhoc_wh TO ROLE dev_adhoc_user;
GRANT USAGE ON WAREHOUSE dev_adhoc_wh to role dev_adhoc_user;

/*--- create different types of warehoues ---*/

/* creating standard warehouse */
CREATE OR REPLACE WAREHOUSE ADHOC_WH WITH WAREHOUSE_SIZE='Small';
/* creating clustered warehouse */
CREATE OR REPLACE WAREHOUSE DEV_POC_WH WITH 
 WAREHOUSE_SIZE='Small'
  MAX_CLUSTER_COUNT = 02
  MIN_CLUSTER_COUNT = 01
  SCALING_POLICY = STANDARD
  AUTO_RESUME = TRUE 
  INITIALLY_SUSPENDED = TRUE
 COMMENT = 'This is a test multi-cluster warehouse'

/* creating snowpark-optimized warehouse */
CREATE OR REPLACE WAREHOUSE DEV_ML_WH 
WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
WAREHOUSE_SIZE='Medium';


/*--- get warehouse metering history ---*/

/*warehouse metering history query */
select sum(credits_used) as total_credits, sum(credits_used_compute) as compute_credits, sum(credits_used_cloud_services) as cloud_services_credits  from account_usage.warehouse_metering_history where datediff(day,cast(start_time as date),current_date) = 60;

/*view sample records from metering history*/
select * from account_usage.warehouse_metering_history limit 10;

/*quarterly usage view */
select sum(credits_used) as total_credits
from account_usage.METERING_HISTORY 
where NAME = 'COMPUTE_WH'
and datediff(day,cast(start_time as date),current_date) = 90;


/*-- Time travel queries -------*/

/* get data as of 5 mins back */

SELECT * FROM RETAIL_DB.POC.SALES AT(OFFSET => -60*5); --to access data at 5 mins back 

/* get data before a query executed using statement --*/

SELECT * FROM RETAIL_DB.POC.SALES BEFORE(STATEMENT => '1e5d0ca7-050e-21e8-b959-a1f5b32c7562');

/*---- Storage queries ---*/

/* storage by date */
SELECT TO_DATE(USAGE_DATE) AS USAGE_DATE, DATABASE_NAME, 
SUM(AVERAGE_DATABASE_BYTES+AVERAGE_FAILSAFE_BYTES)/(1024*1024) AS STORAGE_MB FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY 
GROUP BY TO_DATE(USAGE_DATE), DATABASE_NAME ORDER BY TO_DATE(USAGE_DATE) DESC;

/*storage by date*/

SELECT SUM(STORAGE_BYTES)/(1024*1024) AS TOTAL_STORAGE_MB, SUM(STAGE_BYTES)/(1024*1024) AS STAGE_STORAGE_MB, SUM(FAILSAFE_BYTES)/(1024*1024) AS FAILSAFE_STORAGE_MB
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE 
WHERE USAGE_DATE BETWEEN '2024-01-01' AND '2024-01-31';


