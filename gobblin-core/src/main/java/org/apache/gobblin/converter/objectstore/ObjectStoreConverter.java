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
package gobblin.converter.objectstore;

import gobblin.annotation.Alpha;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.SchemaConversionException;
import gobblin.writer.objectstore.ObjectStoreOperation;

/**
 * A converter of {@link ObjectStoreOperation}s. The output record of any subclasses is of type {@link ObjectStoreOperation}
 *
 * @param <SI> Type of input record schema
 * @param <DI> Input record type
 * @param <DO> Type of {@link ObjectStoreOperation}
 */
@Alpha
public abstract class ObjectStoreConverter<SI, DI, DO extends ObjectStoreOperation<?>> extends Converter<SI, Class<?>, DI, DO> {

  public ObjectStoreConverter<SI, DI, DO> init(WorkUnitState workUnit) {
    return this;
  }

  /**
   * Convert schema is not used this converter hence return the {@link Class} of input schema as a place holder
   * {@inheritDoc}
   * @see gobblin.converter.Converter#convertSchema(java.lang.Object, gobblin.configuration.WorkUnitState)
   */
  @Override
  public Class<?> convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return ObjectStoreOperation.class;
  }
}
