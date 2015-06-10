/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented.converter;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;


/**
 * Instrumented converter that automatically captures certain metrics.
 * Subclasses should implement convertRecordImpl instead of convertRecord.
 *
 * See {@link gobblin.converter.Converter}.
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
