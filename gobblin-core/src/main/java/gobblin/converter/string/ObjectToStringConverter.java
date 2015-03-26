package gobblin.converter.string;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

/**
 * Implementation of {@link Converter} that converts a given {@link Object} to its {@link String} representation
 */
public class ObjectToStringConverter extends Converter<Object, Class<String>, Object, String> {

  @Override
  public Class<String> convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return String.class;
  }

  @Override
  public Iterable<String> convertRecord(Class<String> outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<String>(inputRecord.toString());
  }
}
