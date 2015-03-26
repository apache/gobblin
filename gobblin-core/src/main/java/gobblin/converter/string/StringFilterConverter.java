package gobblin.converter.string;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Optional;
import com.google.common.base.Strings;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;


/**
 * Implementation of {@link Converter} which filters strings based on whether or not they match a regex specified by
 * {@link ConfigurationKeys#CONVERTER_FILTER_STRINGS_BY}
 */
public class StringFilterConverter extends Converter<Class<String>, Class<String>, String, String> {

  private Pattern pattern;
  private Optional<Matcher> matcher;

  @Override
  public Converter<Class<String>, Class<String>, String, String> init(WorkUnitState workUnit) {
    this.pattern =
        Pattern.compile(Strings.nullToEmpty(workUnit.getProp(ConfigurationKeys.CONVERTER_FILTER_STRINGS_BY)));
    this.matcher = Optional.absent();

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

    if (!this.matcher.isPresent()) {
      this.matcher = Optional.of(this.pattern.matcher(inputRecord));
    } else {
      this.matcher.get().reset(inputRecord);
    }

    return this.matcher.get().matches() ? new SingleRecordIterable<String>(inputRecord) : new EmptyIterable<String>();
  }
}
