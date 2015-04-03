/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

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
import gobblin.util.ForkOperatorUtils;


/**
 * Implementation of {@link Converter} which filters strings based on whether or not they match a regex specified by
 * {@link ConfigurationKeys#CONVERTER_STRING_FILTER_PATTERN}
 */
public class StringFilterConverter extends Converter<Class<String>, Class<String>, String, String> {

  private Pattern pattern;
  private Optional<Matcher> matcher;

  @Override
  public Converter<Class<String>, Class<String>, String, String> init(WorkUnitState workUnit) {
    this.pattern =
        Pattern.compile(Strings.nullToEmpty(workUnit.getProp(ForkOperatorUtils.getPropertyNameForBranch(
            workUnit, ConfigurationKeys.CONVERTER_STRING_FILTER_PATTERN))));

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
