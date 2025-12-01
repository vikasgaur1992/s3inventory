# s3inventory

1 - https://chatgpt.com/s/t_692d2cee0e08819187a8611429bc243b
2 - https://chatgpt.com/s/t_692d2c014da08191ae1133ccda9a1675

CREATE DATABASE s3_inventory;
CREATE EXTERNAL TABLE s3_inventory.inventory (
  bucket string,
  key string,
  version_id string,
  is_latest boolean,
  is_delete_marker boolean,
  size bigint,
  last_modified_date timestamp,
  storage_class string,
  etag string,
  encryption_status string,
  replication_status string,
  object_lock_retain_until_date timestamp,
  object_lock_mode string,
  object_lock_legal_hold_status string,
  intelligent_tiering_access_tier string
)
STORED AS PARQUET
LOCATION 's3://my-inventory-reports-bucket-test/reports/'
;

CREATE EXTERNAL TABLE cloudtrail_events (
  eventVersion STRING,
  userIdentity STRUCT<
      type: STRING,
      arn: STRING
  >,
  eventTime TIMESTAMP,
  eventName STRING,
  awsRegion STRING,
  sourceIPAddress STRING,
  userAgent STRING,
  requestParameters STRUCT<
      bucketName: STRING,
      key: STRING
  >
)
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde'
LOCATION 's3://my-inventory-reports-bucket-test/AWSLogss3accesslog/AWSLogs/474727059017/CloudTrail/';
