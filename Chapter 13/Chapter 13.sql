GUZZLE
/* Define a table to store the file metadata */
CREATE DATABASE POC_DB;
CREATE SCHEMA POC;
CREATE WAREHOUSE POC (Proof of concept) size = 'Small';
USE DATABASE POC_DB;
USE SCHEMA POC;

/* create table to store metadata information */
CREATE TABLE image_metadata_table
(
 url string,
 format string,
 size number,
 tags array,
 details string,
 path string
 );

/* load metadata to table using COPY */

COPY INTO image_metadata_table
FROM 
(SELECT $1:url::STRING, $1:format::STRING, $1:size::NUMBER, $1:tag, $1:details::STRING, GET_RELATIVE_PATH(@external_stage, $1:url)
FROM
@external_stage/image_metadata.json)
FILE_FORMAT = (type = json);



/* Query Table*/
SELECT * FROM DIRECTORY(@external_stage);


 /* create view */
CREATE VIEW newspaper_details AS
SELECT
 file_url as paper_url,
 author,
publish_date as publishing_date,
approved_date as approval_date,
 geography,
num_of_pages 
FROM directory(@newspaper_stage) p
JOIN newspaper_metadata n
ON p.file_url = n.file_url;

/* create AWS SNS integration */

CREATE NOTIFICATION INTEGRATION aws_sns_int
ENABLED = TRUE
DIRECTION = OUTBOUND
TYPE = QUEUE
NOTIFICATION_PROVIDER = AWS_SNS
AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-2:xxxxxxxxxxxx:sns_topic'
AWS_SNS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxxxxx:role/sns_role';


USE DATABASE POC_DB;
USE SCHEMA POC;

CREATE OR REPLACE PROCEDURE CLOUDWATCH_LONG_RUN_QUERIES()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
IMMUTABLE
COMMENT='Query snowflake metrics and send metric to snowflake external function for long running queries'
EXECUTE AS CALLER
AS '        result = '''';
                    
        try {
            // change total_elapsed_time >=  3600000 in production
            var smt_get_top_failed_queries = `select * from test`;
            
            var top_failed_queries = snowflake.execute( {sqlText: smt_get_top_failed_queries} );

            while (top_failed_queries.next()) {
                account_id_val = top_failed_queries.getColumnValue(1);
                total_elapsed_time_val = top_failed_queries.getColumnValue(2);

                var smt_update_cloud_watch = 
                    `SELECT SEND_CLOUDWATCH_METRICS_2228(
                     \\''Total_Elapsed_Time\\'',` +
                     total_elapsed_time_val + `,
                     \\''Milliseconds\\'',
                     \\''Snowflake\\'',
                     \\''[{\\\\"Name\\\\": \\\\"Account\\\\",\\\\"Value\\\\": \\\\"`+account_id_val+`\\\\"}]\\''
                    )`;

                snowflake.execute( {sqlText: smt_update_cloud_watch} );

                result = ''Success'';              
            }
        }
        catch (err) {
            result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
            result += "\\n  Message: " + err.message;
            result += "\\nStack Trace:\\n" + err.stackTraceTxt;
        }
          
        return result; 
';
