 /* create Snowflake resources database, warehouse*/
CREATE WAREHOUSE demo_iceberg_wh
WAREHOUSE_TYPE = STANDARD
WAREHOUSE_SIZE = XSMALL;

USE WAREHOUSE demo_iceberg_wh;

/* create database */
CREATE OR REPLACE DATABASE demo_iceberg_db;
CREATE SCHEMA poc;
USE DATABASE demo_iceberg_db;
USE SCHEMA poc;

/* create external volume*/
CREATE OR REPLACE EXTERNAL VOLUME demo_external_volume
 STORAGE_LOCATIONS =
 (
(
NAME = 'demo-s3-us-west-2'
STORAGE_PROVIDER = 'S3'
STORAGE_BASE_URL = 's3://<demo_bucket>/'
STORAGE_AWS_ROLE_ARN = '<arn:aws:iam::xxxxxxxxxxxx:role/demorole>'
STORAGE_AWS_EXTERNAL_ID = 'external_table_id'
 )
);

/* create table */
CREATE OR REPLACE ICEBERG TABLE account_demo_table (
Account_id INTEGER,
acc_name STRING,
acc_address STRING,
acc_zipcd STRING,
acc_phone STRING,
acc_bal NUMERIC,
acc_status STRING,
acc_comment STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'demo_external_volume'
BASE_LOCATION = 'account_demo';

 /* create catalog integration*/
CREATE CATALOG INTEGRATION aws_glue_int
CATALOG_SOURCE=GLUE
CATALOG_NAMESPACE='<catalog-namespace>'
TABLE_FORMAT=ICEBERG
GLUE_AWS_ROLE_ARN='<arn-for-aws-role-to-assume>'
GLUE_CATALOG_ID='<catalog-id>'
GLUE_REGION='<optional-aws-region-of-the-glue-catalog>'
ENABLED=TRUE;

 /* create table */
CREATE ICEBERG TABLE demo_iceberg_table
EXTERNAL_VOLUME='demo_glue_volume'
CATALOG='aws_glue_int'
CATALOG_TABLE_NAME='demo_Glue_Table';

/* CREATE HYBRID table using DDL */
CREATE OR REPLACE HYBRID TABLE cust_info( cust_id NUMBER PRIMARY KEY AUTOINCREMENT START 1 INCREMENT 1, cust_addr VARCHAR NOT NULL, cust_email VARCHAR NOT NULL);

/* CTAS Hybrid table */\
CREATE OR REPLACE HYBRID TABLE customer_demo(
cust_id INT PRIMARY KEY,
dept_id VARCHAR(200)
 )
AS SELECT * FROM customer_details;
