CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`testRecordWithinOptionWithinRecordDDL` ( 
  `parentFieldUnion` struct<`unionRecordMemberFieldLong`:bigint,`unionRecordMemberFieldString`:string> COMMENT 'from flatten_source parentFieldUnion', 
  `parentFieldInt` int COMMENT 'from flatten_source parentFieldInt') 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
LOCATION 
  'file:/user/hive/warehouse/testRecordWithinOptionWithinRecordDDL' 
TBLPROPERTIES ( 
  'columns'='parentFieldUnion,parentFieldInt', 
  'orc.compress'='ZLIB', 
  'columns.types'='struct<unionRecordMemberFieldLong:bigint,unionRecordMemberFieldString:string>,int', 
  'orc.row.index.stride'='268435456')