CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`testRecordWithinRecordWithinRecordDDL` ( 
  `parentFieldRecord__nestedFieldRecord__superNestedFieldString` string COMMENT 'from flatten_source parentFieldRecord.nestedFieldRecord.superNestedFieldString', 
  `parentFieldRecord__nestedFieldRecord__superNestedFieldInt` int COMMENT 'from flatten_source parentFieldRecord.nestedFieldRecord.superNestedFieldInt', 
  `parentFieldRecord__nestedFieldString` string COMMENT 'from flatten_source parentFieldRecord.nestedFieldString', 
  `parentFieldRecord__nestedFieldInt` int COMMENT 'from flatten_source parentFieldRecord.nestedFieldInt', 
  `parentFieldInt` int COMMENT 'from flatten_source parentFieldInt') 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
LOCATION 
  'file:/user/hive/warehouse/testRecordWithinRecordWithinRecordDDL' 
TBLPROPERTIES ( 
  'columns'='parentFieldRecord__nestedFieldRecord__superNestedFieldString,parentFieldRecord__nestedFieldRecord__superNestedFieldInt,parentFieldRecord__nestedFieldString,parentFieldRecord__nestedFieldInt,parentFieldInt', 
  'orc.compress'='ZLIB', 
  'columns.types'='string,int,string,int,int', 
  'orc.row.index.stride'='268435456') 
