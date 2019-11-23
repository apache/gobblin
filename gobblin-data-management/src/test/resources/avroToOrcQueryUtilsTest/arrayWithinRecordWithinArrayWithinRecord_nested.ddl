CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`testArrayWithinRecordWithinArrayWithinRecordDDL` ( 
  `parentRecordFieldName` array<struct<`nestedRecordFieldName`:array<string>>> COMMENT 'from flatten_source parentRecordFieldName') 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
LOCATION 
  'file:/user/hive/warehouse/testArrayWithinRecordWithinArrayWithinRecordDDL' 
TBLPROPERTIES ( 
  'columns'='parentRecordFieldName', 
  'orc.compress'='ZLIB', 
  'columns.types'='array<struct<nestedRecordFieldName:array<string>>>', 
  'orc.row.index.stride'='268435456') 
