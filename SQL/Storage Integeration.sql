-- ============================================================
-- Storage Integration: allow Snowflake to access your S3 bucket
-- ============================================================
USE ROLE ACCOUNTADMIN; 

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_DOT_FLIGHTS_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLE = TRUE
    STORAGE_AWS_ROLE_ARN = '[YOUR AWS ARN]'
    STORAGE_ALLOWED_LOCATIONS = ('[S3 Bucket Name]/dot_flights/');

-- This command shows the AWS IAM info that Snowflake generated.
-- You will need two values from the output for the next step:
--   1. STORAGE_AWS_IAM_USER_ARN
--   2. STORAGE_AWS_EXTERNAL_ID
DESC INTEGRATION S3_DOT_FLIGHTS_INT;
