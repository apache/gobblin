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

package org.apache.gobblin.converter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.WorkUnitState;


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
