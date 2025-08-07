
/* set the context */
Create database POC_DB; --if not exist
create schema POC; --if not exist
use database POC_DB;
use schema POC;

/* Policy to mask the email */
CREATE OR REPLACE MASKING POLICY email_mask AS (val string) returns string ->
CASE
 WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
ELSE '*********'
END;

/* Policy to mask Tax identification number */
CREATE OR REPLACE MASKING POLICY TIN_mask AS (val string) returns string ->
CASE
WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
ELSE NULL
END;

/* mask the name */
CREATE OR REPLACE MASKING POLICY name_mask AS (val string) returns string ->
CASE
WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
ELSE sha2(VAL)
END;


/*create policy for email based on the visibility and role */

create masking policy email_visibility as
(cust_email varchar, visibility string) returns varchar ->
case
when current_role() = 'ACCOUNTADMIN' then cust_email
 when visibility = 'Public' then cust_email
else '***MASKED***'
end;

/* create policy based on the role */
create masking policy email_visibility_policy as
(Val string) returns string ->
case
when current_role() = 'ACCOUNTADMIN' then Val
when current_role() = 'DATA_ANALYST' then regexp_replace(val,'.+@','*****@')
else '*******'
end;


/*Step 1: Create custom role*/

Use role USERADMIN;
/*--create security role--*/
Create role account_security_admin;
/*--Grant permissions to the custom role --*/
GRANT CREATE MASKING POLICY on SCHEMA poc_db.poc to ROLE account_security_admin;
GRANT APPLY MASKING POLICY on ACCOUNT to ROLE account_security_admin;

 /* Grant role created in step 1 to a user*/
GRANT ROLE account_security_admin TO USER poojak;

--Step 3: Create masking policy

/* set context */
USE database poc_db;
USE schema poc;

/*--use new custom role--*/
use role account_security_admin;

/*--use role and create policy--*/
CREATE OR REPLACE MASKING POLICY sensitive_masking AS (val string) returns string ->
 CASE
 WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
 ELSE NULL
END;

/*--create email masking policy-- */
CREATE OR REPLACE MASKING POLICY email_masking AS (val string) returns string ->
CASE
 WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
 ELSE 'cf5 ul ***@**.comcf2 ulnone '
 END;

/*--grant policy to table owner role--*/
GRANT APPLY ON MASKING POLICY sensitive_masking to ROLE data_owners;

GRANT APPLY ON MASKING POLICY email_masking to ROLE data_owners;

--Step 5: Apply masking policy to the database objects


/* create table */
Create table poc_db.poc.cust_info
(
Cust_id int,
Cust_name string,
Cust_email string,
Cust_addr string,
Cust_status string
);

/* apply masking policy to an existing table column */
ALTER TABLE IF EXISTS poc_db.poc.cust_info MODIFY COLUMN cust_addr SET MASKING POLICY sensitive_masking;

/* create view */
Create view poc_db.poc.v_cust_info as select * from poc_db.poc.cust_info;

/* apply the masking policy to a view column */
ALTER VIEW v_cust_info MODIFY COLUMN cust_email SET MASKING POLICY email_masking;

 /* using the ACCOUNTADMIN role */
USE ROLE ACCOUNTADMIN;
USE database poc_db;
USE schema db;

/* run sql query to view customer address and email */
SELECT * from poc_db.poc.cust_info; 

/* use another role */
USE ROLE DATA_ANALYST;
SELECT * from poc_db.poc.cust_info; 

/*capture results of both queries and verify the columns masked */

 /* CREATE TAG Based Policies */


CREATE TAG sensitive_data COMMENT = 'data classification tag';

/* Policy to protect email address */
CREATE OR REPLACE MASKING POLICY 'email_masking' AS(val string) RETURNS string ->
CASE
WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
ELSE '******@***.com'  
END;

/* policy for identification columns */

CREATE OR REPLACE MASKING POLICY 'identity_masking' AS(val string) RETURNS string ->
CASE
WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
ELSE '********'  
END;

 /* assign policies to the tags */

ALTER TAG sensitive_data SET MASKING POLICY email_masking; 
ALTER TAG sensitive_data SET MASKING POLICY identity_masking;

/* assign policies to the objects */
ALTER TABLE poc_db.poc.customer_info SET TAG sensitive_data = 'tag-based policies';
ALTER TABLE poc_db.poc.transaction_details SET TAG sensitive_data = 'tag-based policies';
