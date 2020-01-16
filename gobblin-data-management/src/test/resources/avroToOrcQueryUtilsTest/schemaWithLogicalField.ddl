CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`schemaWithLogicalFieldDDL` (
  `parentFieldRecord` struct<`nestedFieldString`:string,`nestedLogicalFieldDecimal`:decimal(4, 2),`nestedLogicalFieldDate`:date> COMMENT 'from flatten_source parentFieldRecord',
  `parentFieldInt` int COMMENT 'from flatten_source parentFieldInt',
  `parentFieldLogicalVarchar` varchar(256) COMMENT 'from flatten_source parentFieldLogicalVarchar')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'file:/user/hive/warehouse/schemaWithLogicalFieldDDL'
TBLPROPERTIES (
  'columns'='parentFieldRecord,parentFieldInt,parentFieldLogicalVarchar',
  'orc.compress'='ZLIB',
  'columns.types'='struct<nestedFieldString:string,nestedLogicalFieldDecimal:decimal(4,2),nestedLogicalFieldDate:int>,int,varchar(256)',
  'orc.row.index.stride'='268435456')