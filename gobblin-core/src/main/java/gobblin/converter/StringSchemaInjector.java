package gobblin.converter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;


/**
 * Injects a string schema into specified by key {@link #SCHEMA_KEY}.
 */
public class StringSchemaInjector<SI, DI> extends Converter<SI, String, DI, DI> {

  public static final String SCHEMA_KEY = "gobblin.converter.schemaInjector.schema";

  private String schema;

  @Override
  public Converter<SI, String, DI, DI> init(WorkUnitState workUnit) {
    super.init(workUnit);
    Preconditions.checkArgument(workUnit.contains(SCHEMA_KEY));
    this.schema = workUnit.getProp(SCHEMA_KEY);
    return this;
  }

  @Override
  public String convertSchema(SI inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return this.schema;
  }

  @Override
  public Iterable<DI> convertRecord(String outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return Lists.newArrayList(inputRecord);
  }
}
