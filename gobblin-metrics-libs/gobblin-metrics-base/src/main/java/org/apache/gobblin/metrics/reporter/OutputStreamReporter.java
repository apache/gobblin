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
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TimeZone;

import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Closer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


public class OutputStreamReporter extends ConfiguredScheduledReporter {

  private static final String TAGS_SECTION = "-- Tags";
  private static final String GAUGES_SECTION = "-- Gauges";
  private static final String COUNTERS_SECTION = "-- Counters";
  private static final String HISTOGRAMS_SECTION = "-- Histograms";
  private static final String METERS_SECTION = "-- Meters";
  private static final String TIMERS_SECTION = "-- Times";

  private static final Logger LOGGER = LoggerFactory.getLogger(OutputStreamReporter.class);

  public static class Factory {

    public static BuilderImpl newBuilder() {
      return new BuilderImpl();
    }
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  /**
   * A builder for {@link OutputStreamReporter} instances. Defaults to using the default locale and time zone, writing
   * to {@code System.out}, converting rates to events/second, converting durations to milliseconds, and not filtering
   * metrics.
   */
  public static abstract class Builder<T extends ConfiguredScheduledReporter.Builder<T>>
      extends ConfiguredScheduledReporter.Builder<T> {

    protected PrintStream output;
    protected Locale locale;
    protected Clock clock;
    protected TimeZone timeZone;

    protected Builder() {
      this.name = "OutputStreamReporter";
      this.output = System.out;
      this.locale = Locale.getDefault();
      this.clock = Clock.defaultClock();
      this.timeZone = TimeZone.getDefault();
    }

    protected abstract T self();

    /**
     * Write to the given {@link PrintStream}.
     *
     * @param output a {@link PrintStream} instance.
     * @return {@code this}
     */
    public T outputTo(PrintStream output) {
      this.output = output;
      return self();
    }

    /**
     * Write to the given {@link java.io.OutputStream}.
     *
     * @param stream 2 {@link java.io.OutputStream} instance
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

    /**
     * Format numbers for the given {@link Locale}.
     *
     * @param locale a {@link Locale}
     * @return {@code this}
     */
    public T formattedFor(Locale locale) {
      this.locale = locale;
      return self();
    }

    /**
     * Use the given {@link Clock} instance for the time.
     *
     * @param clock a {@link Clock} instance
     * @return {@code this}
     */
    public T withClock(Clock clock) {
      this.clock = clock;
      return self();
    }

    /**
     * Use the given {@link TimeZone} for the time.
     *
     * @param timeZone a {@link TimeZone}
     * @return {@code this}
     */
    public T formattedFor(TimeZone timeZone) {
      this.timeZone = timeZone;
      return self();
    }

    /**
     * Builds a {@link OutputStreamReporter} with the given properties.
     *
     * @return a {@link OutputStreamReporter}
     */
    public OutputStreamReporter build(Properties props) {
      return new OutputStreamReporter(this, ConfigUtils.propertiesToConfig(props,
          Optional.of(ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX)));
    }
  }

  private static final int CONSOLE_WIDTH = 80;

  private final PrintStream output;
  private final Locale locale;
  private final Clock clock;
  private final DateFormat dateFormat;
  private final ByteArrayOutputStream outputBuffer;
  private final PrintStream outputBufferPrintStream;
  private final Closer closer;

  private OutputStreamReporter(Builder<?> builder, Config config) {
    super(builder, config);
    this.closer = Closer.create();
    this.output = this.closer.register(builder.output);
    this.locale = builder.locale;
    this.clock = builder.clock;
    this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, locale);
    this.dateFormat.setTimeZone(builder.timeZone);
    this.outputBuffer = new ByteArrayOutputStream();
    try {
      this.outputBufferPrintStream =
          this.closer.register(new PrintStream(this.outputBuffer, false, Charsets.UTF_8.toString()));
    } catch (UnsupportedEncodingException re) {
      throw new RuntimeException("This should never happen.", re);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      this.closer.close();
    } catch (IOException exception) {
      LOGGER.warn("Failed to close output streams.");
    } finally {
      super.close();
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected synchronized void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers,
      Map<String, Object> tags) {

    this.outputBuffer.reset();

    final String dateTime = dateFormat.format(new Date(clock.getTime()));
    printWithBanner(dateTime, '=');
    this.outputBufferPrintStream.println();

    Map<String, Object> allTags = Maps.newHashMap();
    allTags.putAll(tags);
    allTags.putAll(this.tags);

    if (!allTags.isEmpty()) {
      printWithBanner(TAGS_SECTION, '-');
      for (Map.Entry<String, Object> entry : allTags.entrySet()) {
        this.outputBufferPrintStream.println(String.format("%s=%s", entry.getKey(), entry.getValue()));
      }
      this.outputBufferPrintStream.println();
    }

    if (!gauges.isEmpty()) {
      printWithBanner(GAUGES_SECTION, '-');
      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        this.outputBufferPrintStream.println(entry.getKey());
        printGauge(entry);
      }
      this.outputBufferPrintStream.println();
    }

    if (!counters.isEmpty()) {
      printWithBanner(COUNTERS_SECTION, '-');
      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        this.outputBufferPrintStream.println(entry.getKey());
        printCounter(entry);
      }
      this.outputBufferPrintStream.println();
    }

    if (!histograms.isEmpty()) {
      printWithBanner(HISTOGRAMS_SECTION, '-');
      for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
        this.outputBufferPrintStream.println(entry.getKey());
        printHistogram(entry.getValue());
      }
      this.outputBufferPrintStream.println();
    }

    if (!meters.isEmpty()) {
      printWithBanner(METERS_SECTION, '-');
      for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        this.outputBufferPrintStream.println(entry.getKey());
        printMeter(entry.getValue());
      }
      this.outputBufferPrintStream.println();
    }

    if (!timers.isEmpty()) {
      printWithBanner(TIMERS_SECTION, '-');
      for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        this.outputBufferPrintStream.println(entry.getKey());
        printTimer(entry.getValue());
      }
      this.outputBufferPrintStream.println();
    }

    this.outputBufferPrintStream.println();
    this.outputBufferPrintStream.flush();
    try {
      this.outputBuffer.writeTo(this.output);
    } catch (IOException exception) {
      LOGGER.warn("Failed to write metric report to output stream.");
    }
  }

  private void printMeter(Meter meter) {
    this.outputBufferPrintStream.printf(locale, "             count = %d%n", meter.getCount());
    this.outputBufferPrintStream.printf(locale, "         mean rate = %2.2f events/%s%n", convertRate(meter.getMeanRate()), getRateUnit());
    this.outputBufferPrintStream.printf(locale, "     1-minute rate = %2.2f events/%s%n", convertRate(meter.getOneMinuteRate()), getRateUnit());
    this.outputBufferPrintStream.printf(locale, "     5-minute rate = %2.2f events/%s%n", convertRate(meter.getFiveMinuteRate()), getRateUnit());
    this.outputBufferPrintStream.printf(locale, "    15-minute rate = %2.2f events/%s%n", convertRate(meter.getFifteenMinuteRate()), getRateUnit());
  }

  private void printCounter(Map.Entry<String, Counter> entry) {
    this.outputBufferPrintStream.printf(locale, "             count = %d%n", entry.getValue().getCount());
  }

  private void printGauge(Map.Entry<String, Gauge> entry) {
    this.outputBufferPrintStream.printf(locale, "             value = %s%n", entry.getValue().getValue());
  }

  private void printHistogram(Histogram histogram) {
    this.outputBufferPrintStream.printf(locale, "             count = %d%n", histogram.getCount());
    Snapshot snapshot = histogram.getSnapshot();
    this.outputBufferPrintStream.printf(locale, "               min = %d%n", snapshot.getMin());
    this.outputBufferPrintStream.printf(locale, "               max = %d%n", snapshot.getMax());
    this.outputBufferPrintStream.printf(locale, "              mean = %2.2f%n", snapshot.getMean());
    this.outputBufferPrintStream.printf(locale, "            stddev = %2.2f%n", snapshot.getStdDev());
    this.outputBufferPrintStream.printf(locale, "            median = %2.2f%n", snapshot.getMedian());
    this.outputBufferPrintStream.printf(locale, "              75%% <= %2.2f%n", snapshot.get75thPercentile());
    this.outputBufferPrintStream.printf(locale, "              95%% <= %2.2f%n", snapshot.get95thPercentile());
    this.outputBufferPrintStream.printf(locale, "              98%% <= %2.2f%n", snapshot.get98thPercentile());
    this.outputBufferPrintStream.printf(locale, "              99%% <= %2.2f%n", snapshot.get99thPercentile());
    this.outputBufferPrintStream.printf(locale, "            99.9%% <= %2.2f%n", snapshot.get999thPercentile());
  }

  private void printTimer(Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();
    this.outputBufferPrintStream.printf(locale, "             count = %d%n", timer.getCount());
    this.outputBufferPrintStream.printf(locale, "         mean rate = %2.2f calls/%s%n", convertRate(timer.getMeanRate()), getRateUnit());
    this.outputBufferPrintStream.printf(locale, "     1-minute rate = %2.2f calls/%s%n", convertRate(timer.getOneMinuteRate()), getRateUnit());
    this.outputBufferPrintStream.printf(locale, "     5-minute rate = %2.2f calls/%s%n", convertRate(timer.getFiveMinuteRate()), getRateUnit());
    this.outputBufferPrintStream.printf(locale, "    15-minute rate = %2.2f calls/%s%n", convertRate(timer.getFifteenMinuteRate()), getRateUnit());

    this.outputBufferPrintStream.printf(locale, "               min = %2.2f %s%n", convertDuration(snapshot.getMin()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "               max = %2.2f %s%n", convertDuration(snapshot.getMax()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "              mean = %2.2f %s%n", convertDuration(snapshot.getMean()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "            stddev = %2.2f %s%n", convertDuration(snapshot.getStdDev()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "            median = %2.2f %s%n", convertDuration(snapshot.getMedian()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "              75%% <= %2.2f %s%n", convertDuration(snapshot.get75thPercentile()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "              95%% <= %2.2f %s%n", convertDuration(snapshot.get95thPercentile()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "              98%% <= %2.2f %s%n", convertDuration(snapshot.get98thPercentile()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "              99%% <= %2.2f %s%n", convertDuration(snapshot.get99thPercentile()), getDurationUnit());
    this.outputBufferPrintStream.printf(locale, "            99.9%% <= %2.2f %s%n", convertDuration(snapshot.get999thPercentile()), getDurationUnit());
  }

  private void printWithBanner(String s, char c) {
    this.outputBufferPrintStream.print(s);
    this.outputBufferPrintStream.print(' ');
    for (int i = 0; i < (CONSOLE_WIDTH - s.length() - 1); i++) {
      this.outputBufferPrintStream.print(c);
    }
    this.outputBufferPrintStream.println();
  }
}
