-- =============================================================
-- DOT Flights Project: Database and Schema Setup
-- =============================================================

-- Use ACCOUNTADMIN role for initial setup. 
-- In production you would create a dedicated role, but for a 
-- personal project, this is fine. 
USE ROLE ACCOUNTADMIN; 

-- Create a dedicated database for this project. 
CREATE DATABASE IF NOT EXISTS DOT_FLIGHTS; 

-- Create the raw schema (where S3 data will land)
CREATE SCHEMA IF NOT EXISTS DOT_FLIGHTS.RAW;

-- Create a warehouse for loading and querying data.
-- XSMALL is the cheapest option - 1 credit per hour.
-- AUTOSUSPEND = 60 means it shuts down after 60 seconds of inactivity.
-- Therefore we only pay when it is actually running.
CREATE WAREHOUSE IF NOT EXISTS DOT_FLIGHTS_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Verify everything was created.
SHOW DATABASES LIKE 'DOT_FLIGHTS';
SHOW SCHEMAS IN DATABASE DOT_FLIGHTS;
SHOW WAREHOUSES LIKE 'DOT_FLIGHTS_WH'; 
