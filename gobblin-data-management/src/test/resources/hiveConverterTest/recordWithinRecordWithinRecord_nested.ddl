CREATE EXTERNAL TABLE IF NOT EXISTS `testdb`.`testtable_orc_nested_staging` (
  `parentFieldRecord` struct<`nestedFieldRecord`:struct<`superNestedFieldString`:string,`superNestedFieldInt`:int>,`nestedFieldString`:string,`nestedFieldInt`:int> COMMENT 'from flatten_source parentFieldRecord',
  `parentFieldInt` int COMMENT 'from flatten_source parentFieldInt')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'file:/tmp/testtable_orc_nested'
TBLPROPERTIES (
  'orc.compress'='ZLIB', 
  'orc.row.index.stride'='268435456')
