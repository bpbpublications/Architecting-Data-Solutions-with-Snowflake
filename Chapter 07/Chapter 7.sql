 /* Step 1: Activate Budgets */
CALL snowflake.local.account_root_budget!ACTIVATE();
/* Step 2: Assign limits to budget */
CALL snowflake.local.account_root_budget!SET_SPENDING_LIMIT(2500);

/* Create notification integration */
CREATE NOTIFICATION INTEGRATION budgets_notify_integration
TYPE=EMAIL
ENABLED=TRUE
ALLOWED_RECIPIENTS=('poojakelgaonkar@xyz.com','accountadmin@example.com');

GRANT USAGE ON INTEGRATION budgets_notifiy_integration
TO APPLICATION snowflake;

/* Assign the email notification to the Snowflake budgets:*/

CALL snowflake.local.account_root_budget!SET_EMAIL_NOTIFICATIONS(
'poojakelgaonkar@xyz.com','accountadmin@example.com');

/* create database and schema for budgets */
create database snowflake_budgets;
create schema budgets;

use database snowflake_budgets;
use schema budgets;

/* Create custom budget */
CREATE SNOWFLAKE.CORE.BUDGET dev_budget();

/* assign spend limit */
CALL dev_budget!SET_SPENDING_LIMIT(50);

/* assign notification integration */
CALL dev_budget!SET_EMAIL_NOTIFICATIONS('budgets_notify_integration', '{{{HYPERLINK "mailto:poojakelgaonkar@xyz.com"}}{poojakelgaonkar@xyz.com}}');

/* assign resources to custom budgets */
CALL snowflake_budgets.budgets.dev_budget!ADD_RESOURCE(
SYSTEM$REFERENCE('WAREHOUSE', 'DEV_POC_WH', 'SESSION', 'applybudget'));
