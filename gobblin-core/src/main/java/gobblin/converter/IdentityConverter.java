package gobblin.converter;

import gobblin.configuration.WorkUnitState;

/**
 * Implementation of {@link Converter} that returns the inputSchema unmodified and each inputRecord unmodified
 */
public class IdentityConverter extends Converter<Object, Object, Object, Object> {

  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<Object> convertRecord(Object outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<Object>(inputRecord);
  }
}
