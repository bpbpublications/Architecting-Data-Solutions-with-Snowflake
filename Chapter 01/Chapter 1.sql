/*------------------------Creating and Managing Snowflake accounts  --------------- */

Example 1: Set up an account with a new region and cloud provider.
In this scenario, consider that you want to set up the account in the AWS US WEST 2 region within the organization:
Use role ORGADMIN;
Create account sales_account
  admin_name = admin
  admin_password = 'Demp@2024'
  first_name = Pooja
  last_name = Kelgaonkar
  email = 'pk_email@org.com'
  edition = enterprise
  region = aws_us_west_2;

Example 2: Set up an account in an existing region.
Consider that you already have an account in the AWS region of AWS US EAST 1 and want to set up a new account using the ORGADMIN role in the same region:
Use role ORGADMIN;
create account hr_account
  admin_name = admin
  admin_password = 'Demo2024'
  email = 'pk_gmail@org.com'
  edition = enterprise;

Example 3: Delete accounts

USE ROLE ORGADMIN;
DROP ACCOUNT hr_account GRACE_PERIOD_IN_DAYS = 14; 
DROP ACCOUNT hr_account; --without grace period 


/*------------------------Using Snowsql to connect to Snowflake and Running Queries  --------------- */

/* Connecting to Snowflake using Snowsql */

snowsql -a accountname -u poojak2024 -P -d POC_DEV_DB -r DATA_ENG

/*Running sql using snowsql */

snowsql -c pk_connection -d poc_dev_db -s poc -q 'select * from sales limit 10' 

/*Executing SQL and writing output to CSV */

snowsql -a accountname -u poojak2024 -f /tmp/sql_export.sql -o output_file=/tmp/sales_report.csv -o quiet=true -o friendly=false -o header=true -o output_format=csv 

/*------------------------Creating Roles and Assigning to users --------------- */

/* create role for data engineers */
USE ACCOUNTADMIN;     --set active role
CREATE ROLE data_eng
COMMENT = 'This role is for engineering team';  --creates role for data engineers
GRANT ROLE SYSADMIN to ROLE data_eng;  
/*grant usage on objects for specific objects*/


/* create role for ML engineers */
CREATE ROLE ml_engineer;  --creates role for data engineers
GRANT ROLE SYSADMIN to ROLE ML_ENG /*grant usage on Snowflake objects in place of SYSADMIN*/
COMMENT = 'This role is for ML team'; 
GRANT OPERATE ON WAREHOUSE dev_ml_wh TO ROLE ml_engineer;


/* create role for Data analysts */ 
CREATE ROLE data_analysts
COMMENT = 'This role is for data analysts team';  --creates role for data engineers
GRANT SELECT ON ALL TABLES IN SCHEMA database.schema to ROLE data_analysts;
GRANT OPERATE ON WAREHOUSE compute_wh TO ROLE data_analysts;

/*assign roles to the user*/
GRANT ROLE data_analysts TO USER jsmith; 
GRANT ROLE ml_engineer TO USER jsmith; 

/* Grant Permissions to objects*/

/*set context */
USE SYSADMIN;
CREATE DATABASE POC_DEV_DB;
CREATE SCHEMA POC;
CREATE or replace WAREHOUSE dev_poc_wh with warehouse_size='XSmall';
CREATE ROLE ml_engineer;

/*grant roles and users */
GRANT USAGE on WAREHOUSE dev_poc_wh to role ml_engineer;
GRANT SELECT on ALL TABLES IN SCHEMA POC to role ml_engineer;

