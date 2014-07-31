package com.linkedin.uif.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.lumos.util.SchemaAttributes;
import com.linkedin.lumos.util.SchemaAttributes.TableType;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.Extract;

/**
 * <p>
 * Annotates avro schema with properties needed for downstream processing including Lumos.  This includes
 * information like Primary Key and Delta fields.
 * </p>
 * @author kgoodhop
 *
 */
public class LumosAttributesConverter extends AvroToAvroConverterBase {

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    SchemaAttributes att = new SchemaAttributes(inputSchema);

    Extract extract = workUnit.getExtract();
    
    att.setTableType(TableType.valueOf(extract.getType().toString()));
    att.setDropDate(extract.getExtractId());

    if (extract.getIsFull()) {
      att.isFull(true);
      att.setFullDropDate(extract.getFullExtractRunTime());
      att.setBeginDate(extract.getFullExtractRunTime());
    }

    if (extract.getIsSharded()){
      att.isSharded(true);
      att.setRealDbName(extract.getShardedRealNameSpace());
    }

    if (extract.getIsSecured()) {
      att.isSecured(true);
      att.setPermissionGroup(extract.getSecurityPermissionGroup());
    }
    
    if (extract.getValidationRecordCount() != -1){
      att.setValidationCount(extract.getValidationRecordCount());
      att.setValidationHighWater(extract.getValidationRecordCountHighWaterMark());
    }

    switch (extract.getType()) {
      case SNAPSHOT_APPEND:
        att.setPrimaryKeyFields(extract.getPrimaryKeys());
        // no break;
      case APPEND_ONLY:
        att.setDeltaFields(extract.getDeltaFields());
        break;
      case SNAPSHOT_ONLY:
        att.isFull(true);
        att.setFullDropDate(extract.getFullExtractRunTime());
    }
    
    return att.getSchema();
  }

  @Override
  public GenericRecord convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return inputRecord;
  }

}
