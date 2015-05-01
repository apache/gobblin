/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.io.Closer;

import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.configuration.State;
import gobblin.metrics.MetricContext;
import gobblin.writer.DataWriter;


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
