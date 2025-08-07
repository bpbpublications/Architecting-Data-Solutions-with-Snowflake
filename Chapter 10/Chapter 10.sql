 /* create file format */
USE DATABASE RETAIL_POC_DB;
USE SCHEMA POC;
CREATE OR REPLACE FILE FORMAT src_file_format
TYPE = CSV
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
;

/* create internal stage */
USE DATABASE RETAIL_POC_DB;
USE SCHEMA POC;
CREATE STAGE src_load_stage;

 /* upload files to internal stage*/
PUT  "file:///C:/download/data/retail_load.csv"}}{\fldrslt \cf4 \ul \ulc4 file:///download/data/retail_load.csv}} @src_load_stage;

COPY INTO retail_load
FROM @src_load_stage
FILE_FORMAT = (FORMAT_NAME = src_file_format)
;

COPY INTO retail_load\
FROM @src_load_stage\
FILE_FORMAT = (FORMAT_NAME = src_file_format)
FORCE = TRUE
ON_ERROR = SKIP_FILE
;

CREATE STORAGE INTEGRATION gcp_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gs://source_dir/source_path1/', 'gs://source_dir/source_path2/');

CREATE STAGE gcp_ext_stage
URL='gs://retail/source_files/'
STORAGE_INTEGRATION = gcp_int;

CREATE OR REPLACE FILE FORMAT json_file_format
TYPE = JSON;


COPY INTO transactions_load
FROM 'gs://retail/source_files/transactions.json'
STORAGE_INTEGRATION = gcp_int
FILE_FORMAT = (FORMAT_NAME = json_file_format)
ON_ERROR = SKIP_FILE_10
;

 create pipe retail pipe as copy into retail_table from @retail_stage;

alter pipe retail_pipe SET PIPE_EXECUTION_PAUSED = true;

/* create external stage*/
USE DATABASE RETAIL_POC_DB;
USE SCHEMA POC;
CREATE STAGE pipe_stage
URL = 's3://retail/load/pipe_files'
STORAGE_INTEGRATION = aws_external_int;


/* create snowflake pipe */
Create retail_poc_db.poc.retail_pipe auto_ingest=true as
copy into retail_poc_db.poc.transactions
from @retail_poc_db.poc.pipe_stage
file_format = (FORMAT_NAME = src_file_format);

copy into @retailstage/sales_monthly.csv.gz from sales
file_format = (type=csv compression='gzip')
single=true
max_file_size=4900000000; --this is maximum size supported 5GB\


/* create stream */
USE DATABASE RETAIL_POC_DB;
USE SCHEMA POC;
CREATE STREAM transaction_stream ON TABLE transactions;


/* create stream and query stream to read data from stream */
CREATE DATABASE DEMO;
USE DATABASE DEMO;
CREATE SCHEMA POC;
USE SCHEMA POC;

/* create a sample table to demo stream capability */
CREATE OR REPLACE TABLE library_membership (
 member_id number(8) NOT NULL,
 mem_name varchar(255) default NULL,
 mem_fees number(3) NULL, 
 mem_type varchar(20)
);

/* create stream to track changes in the table*/
CREATE OR REPLACE STREAM members_changes ON TABLE library_membership;

/* load sample records to the table */
INSERT INTO library_membership VALUES
(1010,'Smith',0, 'Short Term'),
(1011,'Andrew',0, 'Annual'),
(1012,'Angela',0, 'Life Long'),
(1013,'Rose',0, 'Temporary Pass');

/* you can view the stream to check the metadata records and action captured*/\
SELECT * from members_changes;

/* get changes to the membership table */
INSERT INTO library_membership VALUES
(1014,'Shivam',0, 'Annual'),
(1015,'Maggie',0, 'Annual');

/* you can view the stream to check the metadata records and action captured*/
SELECT * from members_changes;

/* Every year the charges are renewed for the membership */
CREATE TABLE MEMBERSHIP_CHARGES( mem_id int, member_type string, membership_fees int);

/* create stream on the table */
CREATE OR REPLACE STREAM members_stream ON TABLE MEMBERSHIP_CHARGES;

/* Add new records to the fees table*/

INSERT INTO MEMBERSHIP_CHARGES(1012,'Life Long',199);
INSERT INTO MEMBERSHIP_CHARGES(1011,'Anual',99);
INSERT INTO MEMBERSHIP_CHARGES(1014,'Anual',99);
INSERT INTO MEMBERSHIP_CHARGES(1015,'Anual',99);
INSERT INTO MEMBERSHIP_CHARGES(1010,'Short Term',49);
INSERT INTO MEMBERSHIP_CHARGES(1013,'Temporary Pass',19);

/* apply the changed fees to the membership */

MERGE INTO library_membership a
USING
MEMBERSHIP_CHARGES b
ON a.member_id = b.mem_id
WHEN MATCHED THEN UPDATE SET a.mem_fees = b.membership_fees;

/* view the stream to check the metadata records and action captured*/
SELECT * from members_changes;

create or replace task change_capture_tsk
schedule = '20 minute'
when
system$stream_has_data('members_stream')
as
merge into library_membership a
using (select var:mem_id id, var:mem_type type, var:membership_fees fees from members_stream) s on a.member_id = s.id
when matched then update set a.mem_fees = s.fees;

CREATE FUNCTION rectangle_area(length FLOAT, breadth FLOAT)
RETURNS FLOAT
AS
$$
 length * breadth
$$
;

CREATE OR REPLACE FUNCTION get_promotions(region varchar )
RETURNS TABLE (promo_code varchar, promo_details varchar, promo_validity varchar)
AS 'select promo_code, promo_details , promo_validity
 from promotions a
 where a.region = region
and promo_status='active'';

select *
 from table(get_promotions('US')) promos
 order by promo_validity asc;

/*set the context*/
USE DATABASE DEMO_POC;
USE SCHEMA POC;

/* create stored procedure */
CREATE OR REPLACE PROCEDURE locate_promo_cd(cust_id integer)
RETURNS TABLE (promo_cd varchar, promo_date date)
LANGUAGE SQL
AS
DECLARE
res RESULTSET DEFAULT (SELECT promo_cd, promo_date FROM promotions WHERE customer_id = :cust_id);
BEGIN\
RETURN TABLE(res);
END;

/* call to execute the SP */
CALL locate_promo_cd(010011);


