-- S3 access WITHOUT access keys: a storage integration lets Snowflake assume an AWS IAM role.
-- STATUS: WRITTEN, NEVER EXECUTED. Placeholders in <angle brackets> must be filled in.
-- Order: (1) create the IAM role in AWS with the policy in docs/aws_iam.md and a temporary trust
-- policy, (2) run this, (3) DESC INTEGRATION and copy STORAGE_AWS_IAM_USER_ARN / EXTERNAL_ID into
-- the role's trust policy.
USE ROLE ACCOUNTADMIN;
CREATE STORAGE INTEGRATION IF NOT EXISTS CARELAKE_S3_INT
  TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'S3' ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<ARN_OF_SNOWFLAKE_READ_ROLE>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/<PREFIX>/ingested/');
GRANT USAGE ON INTEGRATION CARELAKE_S3_INT TO ROLE CARELAKE_LOADER;
DESC INTEGRATION CARELAKE_S3_INT;
