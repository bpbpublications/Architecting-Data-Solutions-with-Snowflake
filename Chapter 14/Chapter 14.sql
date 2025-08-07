 /* Create databases for all business units */
CREATE DATABASE FIN_DB;--database for Finance 
CREATE DATABASE HR_DB;  --database for HR
CREATE DATABASE OPS_DB;  --database for OPS
CREATE DATABASE IT_DB;  --database for IT

/* create Schema for each database domain*/
CREATE SCHEMA FIN_DB.FIN; --database schema for finance 
CREATE SCHEMA FIN_DB.FIN_SHARE; --database schema to share data with other domains
CREATE SCHEMA HR_DB.HR; --database schema for HR
CREATE SCHEMA OPS_DB.OPS; --database schema for Operations
CREATE SCHEMA IT_DB.IT; --database schema for IT

/*create rolesfor FINANCE unit */
CREATE ROLE dw_fin_r; --read only db role 
CREATE ROLE dw_fin_rw;  --read and write role 
CREATE ROLE dw_fin_admin;  --admin role 
CREATE ROLE dw_fin_eng; --engineering role
CREATE ROLE dw_fin_analysts; --analyst role 
CREATE ROLE dw_fin_reader;  --read-only account to share data 

/* grant permissions to the role */
-- Grant read-write permissions on database FIN to dw_fin_rw role.
GRANT USAGE ON DATABASE FIN_DB TO ROLE dw_fin_rw;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FIN_DB TO ROLE dw_fin_rw;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN DATABASE  FIN_DB  TO ROLE dw_fin_rw;

-- Grant read-only permissions on database FIN_DB\'a0 to dw_fin_reader role to share read only data with other domain users 
GRANT USAGE ON DATABASE FIN_DB TO ROLE dw_fin_reader;
GRANT USAGE ON SCHEMA FIN_SHARE IN DATABASE FIN_DB TO ROLE dw_fin_reader;
GRANT SELECT ON ALL VIEWS IN SCHEMA FIN_SHARE TO ROLE dw_fin_reader;

