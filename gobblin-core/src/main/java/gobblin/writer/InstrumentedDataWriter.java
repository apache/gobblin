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

package gobblin.writer;

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


/**
 * Instrumented version of {@link gobblin.writer.DataWriter} automatically capturing certain metrics.
 * Subclasses should implement writeImpl instead of write.
 */
public abstract class InstrumentedDataWriter<D> implements DataWriter<D>, Instrumentable, Closeable {

  protected Closer closer;
  protected MetricContext metricContext;
  protected Meter recordsInMeter;
  protected Meter successfulWriteMeter;
  protected Meter exceptionWriteMeter;
  protected Timer dataWriterTimer;

  public InstrumentedDataWriter(State state) {
    this.closer = Closer.create();

    this.metricContext = this.closer.register(Instrumented.getMetricContext(state, this.getClass()));
    this.recordsInMeter = this.metricContext.meter("gobblin.writer.records.in");
    this.successfulWriteMeter = this.metricContext.meter("gobblin.writer.records.written");
    this.exceptionWriteMeter = this.metricContext.meter("gobblin.writer.records.failed");
    this.dataWriterTimer = this.metricContext.timer("gobblin.writer.timer");
  }

  @Override
  public void write(D record)
      throws IOException {
    try {
      long startTimeNanos = System.nanoTime();
      beforeWrite(record);
      writeImpl(record);
      onSuccessfulWrite(startTimeNanos);
    } catch(IOException exception) {
      onException(exception);
      throw exception;
    }
  }

  /**
   * Called beforeWriting a record.
   * @param record record to write.
   */
  public void beforeWrite(D record) {
    this.recordsInMeter.mark();
  }

  /**
   * Called after a successful write of a record.
   * @param startTimeNanos time at which writing started.
   */
  public void onSuccessfulWrite(long startTimeNanos) {
    this.dataWriterTimer.update(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
    this.successfulWriteMeter.mark();
  }

  /** Called after a failed writing of a record.
   * @param exception exception thrown.
   */
  public void onException(Exception exception) {
    this.exceptionWriteMeter.mark();
  }

  /**
   * Subclasses should implement this instead of {@link gobblin.writer.DataWriter#write}
   */
  public abstract void writeImpl(D record) throws IOException;

  @Override
  public void close()
      throws IOException {
    closer.close();
  }

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }
}
