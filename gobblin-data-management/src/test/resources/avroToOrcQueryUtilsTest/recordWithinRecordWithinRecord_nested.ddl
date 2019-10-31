CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`testRecordWithinRecordWithinRecordDDL` ( 
  `parentFieldRecord` struct<`nestedFieldRecord`:struct<`superNestedFieldString`:string,`superNestedFieldInt`:int>,`nestedFieldString`:string,`nestedFieldInt`:int> COMMENT 'from flatten_source parentFieldRecord', 
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
  'columns'='parentFieldRecord,parentFieldInt', 
  'orc.compress'='ZLIB', 
  'columns.types'='struct<nestedFieldRecord:struct<superNestedFieldString:string,superNestedFieldInt:int>,nestedFieldString:string,nestedFieldInt:int>,int', 
  'orc.row.index.stride'='268435456')