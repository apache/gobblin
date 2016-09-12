CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`sourceSchema` ( 
  `parentFieldRecord__nestedFieldRecord__superNestedFieldString` string COMMENT 'from flatten_source parentFieldRecord.nestedFieldRecord.superNestedFieldString', 
  `parentFieldRecord__nestedFieldRecord__superNestedFieldInt` int COMMENT 'from flatten_source parentFieldRecord.nestedFieldRecord.superNestedFieldInt', 
  `parentFieldRecord__nestedFieldString` string COMMENT 'from flatten_source parentFieldRecord.nestedFieldString', 
  `parentFieldInt` int COMMENT 'from flatten_source parentFieldInt', 
  `parentFieldRecord__nestedFieldString2` string COMMENT 'from flatten_source parentFieldRecord.nestedFieldString2') 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
LOCATION 
  'file:/user/hive/warehouse/sourceSchema' 
TBLPROPERTIES ( 
  'orc.compress'='ZLIB', 
  'orc.row.index.stride'='268435456') 
