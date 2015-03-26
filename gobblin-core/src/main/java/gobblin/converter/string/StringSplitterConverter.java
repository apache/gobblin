package gobblin.converter.string;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;

/**
 * Implementation of {@link Converter} that splits a string based on a delimiter specified by
 * {@link ConfigurationKeys#CONVERTER_STRING_SPLITTER_DELIMITER}
 */
public class StringSplitterConverter extends Converter<Class<String>, Class<String>, String, String> {

  private Splitter splitter;

  @Override
  public Converter<Class<String>, Class<String>, String, String> init(WorkUnitState workUnit) {
    Preconditions.checkArgument(workUnit.contains(ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER), "Cannot use "
        + this.getClass().getName() + " with out specifying " + ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER);

    this.splitter =
        Splitter.on(workUnit.getProp(ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER)).omitEmptyStrings();

    return this;
  }

  @Override
  public Class<String> convertSchema(Class<String> inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<String> convertRecord(Class<String> outputSchema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return this.splitter.split(inputRecord);
  }
}
