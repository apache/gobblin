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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.util.ForkOperatorUtils;

/**
 * Implementation of {@link Converter} that splits a string based on a delimiter specified by
 * {@link ConfigurationKeys#CONVERTER_STRING_SPLITTER_DELIMITER}
 */
public class StringSplitterConverter extends Converter<Class<String>, Class<String>, String, String> {

  private Splitter splitter;

  @Override
  public Converter<Class<String>, Class<String>, String, String> init(WorkUnitState workUnit) {
    String stringSplitterDelimiterKey = ForkOperatorUtils.getPropertyNameForBranch(
        workUnit, ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER);

    Preconditions.checkArgument(workUnit.contains(stringSplitterDelimiterKey), "Cannot use "
        + this.getClass().getName() + " with out specifying " + ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER);

    this.splitter =
        Splitter.on(workUnit.getProp(stringSplitterDelimiterKey)).omitEmptyStrings();

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
