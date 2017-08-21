package org.apache.gobblin;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;


/**
 * Test converter which throws DataConversionException while converting first record
 */
public class TestAvroConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  private long recordCount = 0;

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    recordCount++;
    if (recordCount == 1) {
      throw new DataConversionException("Unable to convert record");
    }
    return new SingleRecordIterable<GenericRecord>(inputRecord);
  }
}
