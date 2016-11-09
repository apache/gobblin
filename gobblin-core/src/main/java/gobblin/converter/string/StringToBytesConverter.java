package gobblin.converter.string;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


/**
 * Convert string to bytes using UTF8 encoding.
 */
public class StringToBytesConverter extends Converter<String, String, String, byte[]> {

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<byte[]> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return Lists.newArrayList(inputRecord.getBytes(Charsets.UTF_8));
  }
}
