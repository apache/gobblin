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

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.ForkOperatorUtils;


/**
 * Splits a {@link String} record into a record that is {@link List} of {@link String}, based on
 * {@link ConfigurationKeys#CONVERTER_STRING_SPLITTER_DELIMITER}.
 */
public class StringSplitterToListConverter extends Converter<String, String, String, List<String>> {

  private Splitter splitter;
  private boolean shouldTrimResults;

  @Override
  public Converter<String, String, String, List<String>> init(WorkUnitState workUnit) {
    String stringSplitterDelimiterKey =
        ForkOperatorUtils.getPropertyNameForBranch(workUnit, ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER);
    Preconditions.checkArgument(workUnit.contains(stringSplitterDelimiterKey),
        "Cannot use " + this.getClass().getName() + " with out specifying "
            + ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER);
    this.splitter = Splitter.on(workUnit.getProp(stringSplitterDelimiterKey));
    this.shouldTrimResults = workUnit.getPropAsBoolean(ConfigurationKeys.CONVERTER_STRING_SPLITTER_SHOULD_TRIM_RESULTS,
        ConfigurationKeys.DEFAULT_CONVERTER_STRING_SPLITTER_SHOULD_TRIM_RESULTS);
    return this;
  }

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<List<String>> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    List<String> convertedRecord =
        this.shouldTrimResults ? this.splitter.omitEmptyStrings().trimResults().splitToList(inputRecord)
            : this.splitter.splitToList(inputRecord);
    return new SingleRecordIterable<>(convertedRecord);
  }
}
