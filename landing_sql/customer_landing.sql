CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `lastupdateddate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://udacity-project-work-yw/customer/landing/'
TBLPROPERTIES (
  'classification'='json')
