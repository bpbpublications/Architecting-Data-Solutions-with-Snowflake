
/* LOAD using COPY INTO */
/* set context */
USE DATABASE DEMO_POC_DB;
USE SCHEMA POC;

/* define table DDL */
CREATE TABLE membership_info(info_variant_column VARIANT);

/* COPY INTO table */
COPY INTO membership_info
FROM @load_ext_stage/dataloading/members.json.gz
FILE_FORMAT = (TYPE = 'JSON')
;

 /* Define JSON format to strips the outer array. */
CREATE OR REPLACE FILE FORMAT json_file_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;

/* Define a stage with defined file format */
CREATE OR REPLACE STAGE data_load_stage
FILE_FORMAT = json_format;

/* Copy load files to the stage */
PUT "file:///C:/load/members.json"}}{\fldrslt \cf4 \ul \ulc4 file:///load/members.json}} @data_load_stage AUTO_COMPRESS=TRUE;

/* Load table for the JSON data. */
CREATE OR REPLACE TABLE membership(details VARIANT);

/* Load JSON data files into defined table. */
COPY INTO membership
FROM @data_load_stage/members.json.gz;

/*select from table to view data loaded */
SELECT * FROM membership;

/* SAMPLE JSON */

/* sample JSON format records */
{
 "type": "Lifetime",
 "location": {
 "city": "North York",
 "zip": "140503"
},
"price": "999",
 "purchase_date": "2024-02-16", 
"members": "04"
}
{
"type": "Short-term" ,
 "location": {
 "city": "Toronto",
 "zip": "194278"
},
"price": "99",
 "Purchase_date": "2024-04-02", 
 "members": "02"
}
{
 "type": "Temporary" ,
"location": {
 "city": "Winchester", 
 "zip": "091420"
},
 "price": "19",
"Purchase_date": "2024-03-01", 
 "members": "02"
}

/* UNLOAD DATA using COPY */

 /* copy command to unload data in JSON format */\
COPY INTO @external_stage
FROM 
(
SELECT OBJECT_CONSTRUCT('emp_id', eid, 'first_name', employee_fname, 'last_name', employee_lname, 'Location', city, 'Dateofjoining', DOJ, 'Status',status) 
FROM employees
)
FILE_FORMAT = (TYPE = JSON);

/*export create files on external stage */
/* File exported with data_0_0_0.json.gz in the stage */
{"emp_id":"123456","employee_fname":"Aryan","employee_lname":"Singh","Location":"Toronto","Dateofjoining":"2020-04-01","status":"Active"}
{"emp_id":"671260","employee_fname":"Andrew","employee_lname":"Jacob","Location":"Vancouver","Dateofjoining":"2002-03-10","status":"Active"}

/* Accessing semi structured data */

/*sample SELECT query to access data stored in ARRAY format in a table */\
select acc_info[2] from accounts;

/* query OBJECT format */
select customer_info['customer_id'] from customer;

 /* select data from VARIANT by accessing data with colon*/\
SELECT brand:outlets
FROM sales
ORDER BY 1;

/* credits used for warehouse without date filter*/
SELECT warehouse_name as warehouse_name,
SUM(credits_used_compute) AS credits_consumed
FROM snowflake.account_usage.warehouse_metering_history
GROUP BY 1
ORDER BY 2 DESC;

/* filter credit consumption on date */
SELECT warehouse_name as warehouse_name,
SUM(credits_used_compute) AS credits_consumed
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= current_time()
GROUP BY 1
ORDER BY 2 DESC;

select date_trunc(month, usage_date) as usage_month
 , avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as billable_tb
from storage_usage
group by 1
order by 1; 

 /* create resource monitor */
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR account_warehouse WITH CREDIT_QUOTA=1000
TRIGGERS ON 100 PERCENT DO SUSPEND;

/* assign resource monitor to warehouse */
ALTER WAREHOUSE dev_poc_wh SET RESOURCE_MONITOR = account_warehouse;

