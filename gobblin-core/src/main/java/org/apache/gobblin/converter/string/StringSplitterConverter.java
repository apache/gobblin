/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.converter.string;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.ForkOperatorUtils;

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
