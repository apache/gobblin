CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`schemaWithLogicalFieldDDL` (
  `parentFieldRecord` struct<`nestedFieldString`:string,`nestedLogicalFieldDecimal`:decimal(4, 2),`nestedLogicalFieldDate`:date> COMMENT 'from flatten_source parentFieldRecord',
  `parentFieldInt` int COMMENT 'from flatten_source parentFieldInt',
  `parentFieldLogicalDate` date COMMENT 'from flatten_source parentFieldLogicalDate')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'file:/user/hive/warehouse/schemaWithLogicalFieldDDL'
TBLPROPERTIES (
  'columns'='parentFieldRecord,parentFieldInt,parentFieldLogicalDate',
  'orc.compress'='ZLIB',
  'columns.types'='struct<nestedFieldString:string,nestedLogicalFieldDecimal:decimal(4,2),nestedLogicalFieldDate:int>,int,int',
  'orc.row.index.stride'='268435456')