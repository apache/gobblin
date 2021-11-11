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

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;


/**
 * Implementation of {@link Converter} that converts a given {@link Object} to its {@link String} representation
 */
public class KafkaRecordToStringConverter extends Converter<Object, String, DecodeableKafkaRecord, String> {

  @Override
  public String convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema.toString();
  }

  @Override
  public Iterable<String> convertRecord(String outputSchema, DecodeableKafkaRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<>(inputRecord.getValue().toString());
  }
}
