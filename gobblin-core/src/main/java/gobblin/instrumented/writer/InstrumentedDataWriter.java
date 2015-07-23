/*
 *
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

package gobblin.instrumented.writer;

import java.io.IOException;

import gobblin.configuration.State;


/**
 * Instrumented version of {@link gobblin.writer.DataWriter} automatically capturing certain metrics.
 * Subclasses should implement writeImpl instead of write.
 */
public abstract class InstrumentedDataWriter<D> extends InstrumentedDataWriterBase<D> {

  public InstrumentedDataWriter(State state) {
    super(state);
  }

  @Override
  public final void write(D record)
      throws IOException {
    super.write(record);
  }
}
