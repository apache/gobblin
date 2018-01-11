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

package org.apache.gobblin.metrics.reporter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.reporter.util.AvroJsonSerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.NoopSchemaVersionWriter;


/**
 * {@link org.apache.gobblin.metrics.reporter.EventReporter} that writes {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s to an
 * {@link java.io.OutputStream}.
 */
public class OutputStreamEventReporter extends EventReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OutputStreamEventReporter.class);
  private static final int CONSOLE_WIDTH = 80;

  protected PrintStream output;

  protected final AvroSerializer<GobblinTrackingEvent> serializer;
  private final ByteArrayOutputStream outputBuffer;
  private final PrintStream outputBufferPrintStream;
  private final DateFormat dateFormat;

  public OutputStreamEventReporter(Builder builder) throws IOException {
    super(builder);
    this.serializer = this.closer.register(
        new AvroJsonSerializer<GobblinTrackingEvent>(GobblinTrackingEvent.SCHEMA$, new NoopSchemaVersionWriter()));
    this.output = builder.output;
    this.outputBuffer = new ByteArrayOutputStream();
    this.outputBufferPrintStream = this.closer.register(new PrintStream(this.outputBuffer, false, Charsets.UTF_8.toString()));
    this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, Locale.getDefault());
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {

    if(queue.size() <= 0) {
      return;
    }
    this.outputBuffer.reset();
    GobblinTrackingEvent nextEvent;

    final String dateTime = dateFormat.format(new Date());
    printWithBanner(dateTime, '=');
    this.outputBufferPrintStream.println();
    printWithBanner("-- Events", '-');

    while(null != (nextEvent = queue.poll())) {
      this.outputBufferPrintStream.println(new String(this.serializer.serializeRecord(nextEvent), Charsets.UTF_8));
    }

    this.outputBufferPrintStream.println();
    try {
      this.outputBuffer.writeTo(this.output);
    } catch(IOException exception) {
      LOGGER.warn("Failed to write events to output stream.");
    }
  }

  private void printWithBanner(String s, char c) {
    this.outputBufferPrintStream.print(s);
    this.outputBufferPrintStream.print(' ');
    for (int i = 0; i < (CONSOLE_WIDTH - s.length() - 1); i++) {
      this.outputBufferPrintStream.print(c);
    }
    this.outputBufferPrintStream.println();
  }

  /**
   * Returns a new {@link org.apache.gobblin.metrics.kafka.KafkaEventReporter.Builder} for {@link org.apache.gobblin.metrics.kafka.KafkaEventReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
   * @return KafkaReporter builder
   */
  public static Builder<? extends Builder> forContext(MetricContext context) {
    return new BuilderImpl(context);
  }

  private static class BuilderImpl extends Builder<BuilderImpl> {
    public BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static abstract class Builder<T extends Builder<T>> extends EventReporter.Builder<T> {

    protected PrintStream output;

    public Builder(MetricContext context) {
      super(context);
      this.output = System.out;
    }

    /**
     * Write to the given {@link java.io.PrintStream}.
     *
     * @param output a {@link java.io.PrintStream} instance.
     * @return {@code this}
     */
    public T outputTo(PrintStream output) {
      this.output = output;
      return self();
    }

    /**
     * Write to the given {@link java.io.OutputStream}.
     * @param stream a {@link java.io.OutputStream} instance
     * @return {@code this}
     */
    public T outputTo(OutputStream stream) {
      try {
        this.output = new PrintStream(stream, false, Charsets.UTF_8.toString());
      } catch(UnsupportedEncodingException exception) {
        LOGGER.error("Unsupported encoding in OutputStreamReporter. This is an error with the code itself.", exception);
        throw new RuntimeException(exception);
      }
      return self();
    }

    public OutputStreamEventReporter build() throws IOException {
      return new OutputStreamEventReporter(this);
    }
  }
}
