
  
CREATE WAREHOUSE WEATHEWARE;                                                -- Create Warehouse    
USE WAREHOUSE WEATHEWARE;                                                   -- Select and activate the warehouse

CREATE DATABASE WEATHER_DATA;                                               -- Create database
USE DATABASE WEATHER_DATA;                                                  -- Switch to use the database

CREATE SCHEMA PUBLICS;                                                      -- Creation of Schema


CREATE STORAGE INTEGRATION s3_weatherdata                                   -- Creation of Storage Integration for external s3 access
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/fake-placeholder-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://weatherdataprocessedd/');                -- Allow access only to a particular s3 bucket

DESC INTEGRATION s3_weatherdata;                                            -- Describe the details and security credentials of s3_weatherdata

ALTER STORAGE INTEGRATION s3_weatherdata                                    -- Update the integration to use the correct IAM Role
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::054183072754:role/snowflakes3sqsRole';           -- Set the Real IAM Role arn

CREATE STAGE weather_stagee                                                 -- Creation of an external stage
STORAGE_INTEGRATION = s3_weatherdata                                        -- Link the stage of integration
URL = 's3://weatherdataprocessedd/'
FILE_FORMAT = (TYPE = JSON);

LIST @weather_stagee;                                                       -- List all files available in the s3 stage

SELECT * FROM INFORMATION_SCHEMA.STAGES;



CREATE OR REPLACE TABLE weather_data (                                     -- Creation or Replacement of a table with weather related columns
    location_name STRING,
    "localtime" TIMESTAMP_NTZ,
    inserted_copy_time TIMESTAMP_NTZ,
    original_temp_c FLOAT,
    original_humidity FLOAT,
    dewpoint_c FLOAT,
    condition_text STRING,
    pressure_mb FLOAT,
    country STRING,
    cloud FLOAT,
    feelslike_f FLOAT,
    uv_index FLOAT,
    condition_icon STRING,
    wind_degree FLOAT,
    visibility_miles FLOAT,
    gust_mph FLOAT,
    wind_dir STRING,
    gust_kph FLOAT,
    condition_code STRING,
    windchill_f FLOAT,
    pressure_in FLOAT,
    region STRING,
    feelslike_c FLOAT,
    is_day BOOLEAN,
    latitude FLOAT,
    temp_c FLOAT,
    temp_f FLOAT,
    windchill_c FLOAT,
    wind_kph FLOAT,
    wind_mph FLOAT,
    heatindex_f FLOAT,
    precip_mm FLOAT,
    longitude FLOAT,
    timezone STRING,
    heatindex_c FLOAT,
    visibility_km FLOAT,
    dewpoint_f FLOAT,
    precip_in FLOAT
);

CREATE OR REPLACE PIPE weather_pipe                                       -- Creation or Replacement of a snowpipe
AUTO_INGEST = TRUE 
AS
COPY INTO weather_data                                                    -- Define copyinto  command to load data into 'weather_data' table
FROM @weather_stagee                                                      -- Source from 'weather_data'
FILE_FORMAT = (TYPE = 'JSON')   
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;                                  -- Match JSON keys to table columns case-insensitivity

ALTER PIPE WEATHER_PIPE SET PIPE_EXECUTION_PAUSED=true;                   -- Pause the snowpipe temporarily

GRANT OWNERSHIP ON PIPE weather_pipe TO ROLE SYSADMIN;                    -- Grant ownership of the pipe to SYSADMIN role

ALTER PIPE weather_pipe REFRESH;                                          -- Manually refresh the pipe to recogonize existing files in s3 stage 



CREATE OR REPLACE FILE FORMAT weather_json                                -- Create or replace a named  file format  
TYPE = 'JSON'
STRIP_NULL_VALUES = TRUE                                                  -- Remove null values from JSON objects on load 
IGNORE_UTF8_ERRORS = TRUE;

COPY INTO weather_data
FROM @weather_stagee                                                      -- Manually load data into 'weather_data' table (outside of snowpipe)
FILE_FORMAT = (FORMAT_NAME = 'weather_json')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


SELECT * FROM weather_data;                                              -- Query all records from 'weather_data' table to vrify load 