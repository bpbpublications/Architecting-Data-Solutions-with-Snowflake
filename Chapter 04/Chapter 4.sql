/*----------setting up network rules -------*/

/* allow only specific traffic from given IP ranges */
CREATE NETWORK RULE allowed_traffic_rule
  MODE = INGRESS
  TYPE = IPV4
  VALUE_LIST = ('192.2.1.0/24');

/* block any traffic from public ips*/
CREATE NETWORK RULE blocked_traffic_rule
  MODE = INGRESS
  TYPE = IPV4
  VALUE_LIST = ('0.0.0.0/0');

/* Create policy with network rules*/
CREATE NETWORK POLICY account_nw_policy
  ALLOWED_NETWORK_RULE_LIST = ('allowed_traffic_rule')
  BLOCKED_NETWORK_RULE_LIST=('blocked_traffic_rule');


/*applying policy to account */

USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET NETWORK_POLICY = account_nw_policy;


/* applying policy to a user */

USE ROLE USERADMIN;
ALTER USER smith SET NETWORK_POLICY = account_user_policy;



/*-------setting data retention ----*/

/* setup a table data retention to 90 days */
CREATE TABLE poc_db.poc.store_info(store_nm String,Store_no NUMBER, Store_loc String, Store_start_date DATE, Store_close_dt DATE) DATA_RETENTION_TIME_IN_DAYS=90;
/* Modifying the data retention for existing table */
ALTER TABLE  poc_db.poc.store_info SET DATA_RETENTION_TIME_IN_DAYS=30;


/*------------------------Time travel ----------------------------*/

/* accessing historical data using time travel with AT clause */
SELECT * FROM poc_db.poc.store_info AT(TIMESTAMP => 'Mon, 26 Feb 2024 05:30:00 -0700'::timestamp_tz);

/* accessing historical data using a query offset or query that is run 10 minutes back */
SELECT * FROM poc_db.poc.store_info  AT(OFFSET => -60*10); --OFFSET is always in the form of seconds

/* Restore or access data before a query is executed - using query id */
SELECT * FROM poc_db.poc.store_info BEFORE(STATEMENT => '2e7d0ca5-006e-33e6-b354-a8f4b29c2618');

/* Clone a table using AT clause and a timestamp*/
CREATE TABLE poc_db.poc.store_info_backup CLONE poc_db.poc.store_info 
  AT(TIMESTAMP => 'Mon, 26 Feb 2024 05:30:00 -0700'::timestamp_tz);

/* restore dropped database objects */
undrop database poc_db;
undrop schema poc;
undrop table store_info;

/*--------------------Cloning data-----------------------*/

CREATE REPLICATION GROUP replicate_poc_db
    OBJECT_TYPES = DATABASES
    ALLOWED_DATABASES = DEV_POC_DB, DEMO_DB
    ALLOWED_ACCOUNTS = org.account2, org.account3
    REPLICATION_SCHEDULE = '30 MINUTE'; /* this is used to automate the refresh */

/*Manually refresh from target account */

ALTER REPLICATION GROUP replicate_poc_db REFRESH;

