CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`sourceSchema` ( 
  `parentFieldRecord__nestedFieldRecord__superNestedFieldString` string COMMENT '', 
  `parentFieldRecord__nestedFieldRecord__superNestedFieldInt` int COMMENT '', 
  `parentFieldRecord__nestedFieldString` string COMMENT '', 
  `parentFieldInt` int COMMENT '', 
  `parentFieldRecord__nestedFieldString2` string COMMENT '') 
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
