 /* DECLARE EXCEPTIONS */
DECLARE
sp_exception EXCEPTION (-20002, 'Raised SP_EXCEPTION.');
BEGIN
LET counter := 0;
LET should_raise_exception := true;
IF (should_raise_exception) THEN
RAISE sp_exception;
END IF;
counter := counter + 1;
RETURN counter;
END;


/* Audit Batch Control table*/
CREATE DATABASE AUDIT_DB;
CREATE SCHEMA AUDIT;


/* SET CONTEXT*/
USE DATABASE AUDIT_DB;
USE SCHEMA AUDIT;

/* CREATE CONTROL TABLE */
CREATE TABLE AUDIT_BTCH_CTL
(
SRC_SYS_CD STRING,
SRC_SYS_NM STRING,
JOB_ID INT,
JOB_NAME STRING,
JOB_TYPE STRING,
STATUS STRING
);

/* set the context */
USE DATABASE AUDIT_DB;
USE SCHEMA AUDIT;

/* CREATE BATCH LOG TABLE */
CREATE BATCH_LOG_TBL
(
BATCH_ID INT,
JOB_ID INT,
JOB_NAME STRING,
JOB_STRT_TS TIMESTAMP,
JOB_END_TS TIMESTAMP,
JOB_STATUS STRING,
ERROR_CD STRING,
ERROR_MSG STRING,
BATCH_USR_ID STRING
 );

/* create event table*/
Create database log_db;
Use database log_db;
Create schema logs;
Use schema logs;
CREATE EVENT TABLE log_events;

/* associate event table with an account */
ALTER ACCOUNT SET EVENT_TABLE = log_db.logs.log_events;

/* set the log level for database and UDF (user defined functions) */
USE ROLE ACCOUNTADMIN;
ALTER DATABASE RETAIL_DB_POC SET LOG_LEVEL = ERROR;
ALTER FUNCTION demo_function(int) SET LOG_LEVEL = WARN;

/* create role and grant permissions*/
CREATE ROLE LOG_ADMIN;
GRANT MODIFY LOG LEVEL ON ACCOUNT TO ROLE LOG_ADMIN;

/*grant log_admin to the engineering role*/
GRANT ROLE LOG_ADMIN to ROLE DATA_ENG;

/* use DATA ENG role to develop the code and test*/
USE ROLE DATA_ENG;
ALTER SESSION SET LOG_LEVEL = DEBUG;

/* Raising exceptions */\

/* Sample to set the log level to error, warning, debug, trace, and fatal error */ 
SYSTEM$LOG_ERROR('Error message');
SYSTEM$LOG_WARN('Warning message');
SYSTEM$LOG_DEBUG('Debug message');
SYSTEM$LOG_TRACE('Trace message');
SYSTEM$LOG_FATAL('Fatal message');

/* use below function to pass the corresponding message and log level */
SYSTEM$LOG('error', 'Error message');
SYSTEM$LOG('warning', 'Warning message');

/* Adding two events */
SYSTEM$ADD_EVENT('SProcEmptyEvent');

/* adds event with an attribute specified in key-value pair*/
SYSTEM$ADD_EVENT('SProcEventWithAttributes', \{'key1': 'value1', 'key2': 'value2'\});

CREATE OR REPLACE PROCEDURE demo_pi_proc()
RETURNS DOUBLE
LANGUAGE SQL
AS $$
BEGIN
 -- Add an event without attributes
SYSTEM$ADD_EVENT('pi_testing');

-- Add an event with attributes
 LET attr := \{'score': 89, 'pass': TRUE\};
 SYSTEM$ADD_EVENT('pi_testing', attr);

 -- Set attributes for the span
 SYSTEM$SET_SPAN_ATTRIBUTES({'key1': 'value1', 'key2': TRUE\});

RETURN 3.14;
END;
$$;

/* set the event table name */
SET event_table_name='log_db.logs.log_events';

/* use SELECT to query the log data*/
SELECT
TIMESTAMP as time,
RESOURCE_ATTRIBUTES['snow.executable.name'] as log_executable,
RECORD['severity_text'] as log_severity,
VALUE as log_message
FROM
IDENTIFIER($event_table_name)
WHERE
RESOURCE_ATTRIBUTES['snow.executable.name'] LIKE '%demo_pi_proc%'
AND RECORD_TYPE = 'LOG';


create notification integration app_email_int
    type=email
    enabled=true
    allowed_recipients=('firstname.lastname@xyz.com')
;


CALL SYSTEM$SEND_EMAIL(
    'app_email_int',
    'first_name.last_name@example.com, firstname.lastname@example.com',
    'Email Alert: Pipeline completed Successfully.',
    'Data load job has successfully finished.\nStart Time: 08:00:00\nEnd Time: 11:35:15\nTotal Records Processed: 214567'
);




/* create task to send notification and log alerts using SP */
CREATE TASK log_monitor
  SCHEDULE = '15 MINUTE'
  ERROR_INTEGRATION = log_notification_int
  AS
 call snowflake_alerts_sp();

/* alter the task to set the notification if already exists*/
ALTER TASK log_monitor 
SET ERROR_INTEGRATION = log_notification_int;

/* create integration*/
CREATE NOTIFICATION INTEGRATION log_notification_int
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = OUTBOUND
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-2:111122223333:sns_topic'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::111122223333:role/error_sns_role';

  
/* get null count */

SELECT SNOWFLAKE.CORE.NULL_COUNT(
  SELECT
    Cust_id
  FROM retail.public.cust_info
);

/*get unique count */
SELECT SNOWFLAKE.CORE.UNIQUE_COUNT(
  SELECT
    Emp_id
  FROM hr.tables.empl_info
);


/* create custom function */

CREATE OR REPLACE DATA METRIC FUNCTION dq_checks.public.count_valid_values(
  Input_t TABLE(
    inp_c1 NUMBER,
    inp_c2 NUMBER,
    inp_c3 NUMBER
  )
)
RETURNS NUMBER
AS
$$
  SELECT
    COUNT(*)
  FROM sales
  WHERE
    inp_c1>0
    AND inp_c2>0
    AND inp_c3>0
$$;

/* grant usage */

GRANT USAGE ON FUNCTION
  dq_checks.public.count_valid_values(TABLE(NUMBER, NUMBER, NUMBER))
  TO dev_data_eng;

  /* apply to table */

  ALTER TABLE retail.public.sales SET
  DATA_METRIC_SCHEDULE = '5 MINUTE';

ALTER TABLE retail.public.sales
  ADD DATA METRIC FUNCTION dq_checks.public.count_valid_values
  ON (c1, c2, c3);


  
