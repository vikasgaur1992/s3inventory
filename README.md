# s3inventory

1 - https://chatgpt.com/s/t_692d2cee0e08819187a8611429bc243b
2 - https://chatgpt.com/s/t_692d2c014da08191ae1133ccda9a1675
####################################################
CREATE DATABASE IF NOT EXISTS s3_inventory;
DROP TABLE IF EXISTS s3_inventory.all_objects;

CREATE EXTERNAL TABLE s3_inventory.all_objects (
  bucket STRING,
  key STRING,
  version_id STRING,
  is_latest BOOLEAN,
  is_delete_marker BOOLEAN,
  size BIGINT,
  last_modified_date TIMESTAMP,
  e_tag STRING,
  storage_class STRING,
  is_multipart_uploaded BOOLEAN,
  replication_status STRING,
  encryption_status STRING,
  object_lock_retain_until_date TIMESTAMP,
  object_lock_mode STRING,
  object_lock_legal_hold_status STRING,
  intelligent_tiering_access_tier STRING,
  bucket_key_status STRING,
  checksum_algorithm STRING,
  last_accessed_date DATE
)
STORED AS PARQUET
LOCATION 's3://my-inventory-reports-bucket-test/report/autogiro-artifacts/daily-inventory/data/';

SELECT COUNT(*) FROM s3_inventory.all_objects;

SELECT
  key,
  last_accessed_date
FROM s3_inventory.all_objects
LIMIT 200;

SELECT
  COUNT(*) AS total_objects,
  COUNT(last_accessed_date) AS objects_with_access_date
FROM s3_inventory.all_objects;

SELECT
  key,
  size,
  storage_class,
  last_accessed_date
FROM s3_inventory.all_objects
WHERE
  last_accessed_date < current_date - interval '180' day
ORDER BY last_accessed_date ASC;

SELECT
  key,
  size,
  storage_class
FROM s3_inventory.all_objects
WHERE last_accessed_date IS NULL;

SELECT
  key,
  size,
  storage_class,
  last_accessed_date
FROM s3_inventory.all_objects
WHERE last_accessed_date IS NULL;
###################################################
CREATE DATABASE IF NOT EXISTS cloudtrail_logs;

CREATE EXTERNAL TABLE cloudtrail_logs.cloudtrail_s3 (
  eventVersion STRING,
  userIdentity STRUCT<
    type: STRING,
    principalId: STRING,
    arn: STRING,
    accountId: STRING,
    accessKeyId: STRING,
    userName: STRING
  >,
  eventTime STRING,
  eventSource STRING,
  eventName STRING,
  awsRegion STRING,
  sourceIPAddress STRING,
  userAgent STRING,
  requestParameters STRUCT<
    bucketName: STRING,
    key: STRING
  >,
  responseElements STRING,
  additionalEventData STRING,
  requestID STRING,
  eventID STRING,
  readOnly BOOLEAN,
  resources ARRAY<STRUCT<
    ARN: STRING,
    accountId: STRING,
    type: STRING
  >>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://my-inventory-reports-bucket-test/AWSLogss3accesslog/AWSLogs/474727059017/CloudTrail/';

#Verify CloudTrail data exists
SELECT eventName, COUNT(*)
FROM cloudtrail_logs.cloudtrail_s3
GROUP BY eventName;

#Identify objects accessed in last 30 days
SELECT DISTINCT
  requestParameters.bucketName AS bucket,
  requestParameters.key        AS key
FROM cloudtrail_logs.cloudtrail_s3
WHERE
  eventSource = 's3.amazonaws.com'
  AND eventName IN ('GetObject','HeadObject')
  AND from_iso8601_timestamp(eventTime)
      >= current_timestamp - interval '1' day
  AND requestParameters.bucketName = 'autogiro-artifacts';
  
 #Find inventory objects NOT accessed in last 30 days (JOIN) 
  SELECT
  i.key,
  i.size,
  i.storage_class
FROM s3_inventory.all_objects i
LEFT JOIN (
  SELECT DISTINCT requestParameters.key AS key
  FROM cloudtrail_logs.cloudtrail_s3
  WHERE
    eventSource = 's3.amazonaws.com'
    AND eventName IN ('GetObject','HeadObject')
    AND from_iso8601_timestamp(eventTime)
        >= current_timestamp - interval '30' day
    AND requestParameters.bucketName = 'autogiro-artifacts'
) c
ON i.key = c.key
WHERE c.key IS NULL;
#Add last_modified_date from inventory
SELECT
  i.key,
  i.size,
  i.storage_class,
  i.last_modified_date,
  i.last_accessed_date
FROM s3_inventory.all_objects i
LEFT JOIN (
  SELECT DISTINCT requestParameters.key AS key
  FROM cloudtrail_logs.cloudtrail_s3
  WHERE
    eventSource = 's3.amazonaws.com'
    AND eventName IN ('GetObject','HeadObject')
    AND from_iso8601_timestamp(eventTime)
        >= current_timestamp - interval '30' day
    AND requestParameters.bucketName = 'autogiro-artifacts'
) c
ON i.key = c.key
WHERE c.key IS NULL;

SELECT
  requestParameters.key AS key,
  MAX(from_iso8601_timestamp(eventTime)) AS last_access_time
FROM cloudtrail_logs.cloudtrail_s3
WHERE
  eventSource = 's3.amazonaws.com'
  AND eventName IN ('GetObject','HeadObject')
  AND requestParameters.bucketName = 'autogiro-artifacts'
GROUP BY requestParameters.key;
#If you want “last accessed time EVER” (not just 30 days)
SELECT
  i.key,
  i.size,
  i.storage_class,
  i.last_modified_date,
  c.last_access_time
FROM s3_inventory.all_objects i
LEFT JOIN (
  SELECT
    requestParameters.key AS key,
    MAX(from_iso8601_timestamp(eventTime)) AS last_access_time
  FROM cloudtrail_logs.cloudtrail_s3
  WHERE
    eventSource = 's3.amazonaws.com'
    AND eventName IN ('GetObject','HeadObject')
    AND requestParameters.bucketName = 'autogiro-artifacts'
  GROUP BY requestParameters.key
) c
ON i.key = c.key;
############################################
Test tommorow
SELECT DISTINCT requestParameters.key AS key
FROM cloudtrail_logs.cloudtrail_s3
WHERE
  eventSource = 's3.amazonaws.com'
  AND eventName IN ('GetObject', 'HeadObject')
  AND requestParameters.bucketName = 'autogiro-artifacts'
  AND from_iso8601_timestamp(eventTime) >= current_date
  AND requestParameters.key = 'autogiro-api/worker/.classpath';
