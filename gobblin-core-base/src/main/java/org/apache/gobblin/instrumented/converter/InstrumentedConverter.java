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

package org.apache.gobblin.instrumented.converter;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;


/**
 * Instrumented converter that automatically captures certain metrics.
 * Subclasses should implement convertRecordImpl instead of convertRecord.
 *
 * See {@link org.apache.gobblin.converter.Converter}.
 *
 * @author ibuenros
 */
public abstract class InstrumentedConverter<SI, SO, DI, DO> extends InstrumentedConverterBase<SI, SO, DI, DO> {

  @Override
  public final Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return super.convertRecord(outputSchema, inputRecord, workUnit);
  }

}
