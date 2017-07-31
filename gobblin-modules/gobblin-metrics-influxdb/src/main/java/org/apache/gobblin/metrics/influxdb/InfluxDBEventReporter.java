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

package org.apache.gobblin.metrics.influxdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.MultiPartEvent;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.reporter.EventReporter;

import static org.apache.gobblin.metrics.event.TimingEvent.METADATA_DURATION;

/**
 *
 * {@link org.apache.gobblin.metrics.reporter.EventReporter} that emits {@link org.apache.gobblin.metrics.GobblinTrackingEvent} events
 * as timestamped name - value pairs to InfluxDB
 *
 * @author Lorand Bendig
 *
 */
public class InfluxDBEventReporter extends EventReporter {

  private final InfluxDBPusher influxDBPusher;

  private static final double EMTPY_VALUE = 0d;
  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBEventReporter.class);

  public InfluxDBEventReporter(Builder<?> builder) throws IOException {
    super(builder);
    if (builder.influxDBPusher.isPresent()) {
      this.influxDBPusher = builder.influxDBPusher.get();
    } else {
      this.influxDBPusher = new InfluxDBPusher.Builder(builder.url, builder.username, builder.password,
          builder.database, builder.connectionType).build();
    }
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {

    GobblinTrackingEvent nextEvent;
    try {
      while (null != (nextEvent = queue.poll())) {
        pushEvent(nextEvent);
      }
    } catch (IOException e) {
      LOGGER.error("Error sending event to InfluxDB", e);
    }
  }

  /**
   * Extracts the event and its metadata from {@link GobblinTrackingEvent} and creates
   * timestamped name value pairs
   *
   * @param event {@link GobblinTrackingEvent} to be reported
   * @throws IOException
   */
  private void pushEvent(GobblinTrackingEvent event) throws IOException {

    Map<String, String> metadata = event.getMetadata();
    String name = getMetricName(metadata, event.getName());
    long timestamp = event.getTimestamp();
    MultiPartEvent multiPartEvent = MultiPartEvent.getEvent(metadata.get(EventSubmitter.EVENT_TYPE));
    if (multiPartEvent == null) {
      influxDBPusher.push(buildEventAsPoint(name, EMTPY_VALUE, timestamp));
    }
    else {
      List<Point> points = Lists.newArrayList();
      for (String field : multiPartEvent.getMetadataFields()) {
        Point point = buildEventAsPoint(JOINER.join(name, field), convertValue(field,  metadata.get(field)), timestamp);
        points.add(point);
      }
      influxDBPusher.push(points);
    }
  }

  /**
   * Convert the event value taken from the metadata to double (default type).
   * It falls back to string type if the value is missing or it is non-numeric
   * is of string or missing
   * Metadata entries are emitted as distinct events (see {@link MultiPartEvent})
   *
   * @param field {@link GobblinTrackingEvent} metadata key
   * @param value {@link GobblinTrackingEvent} metadata value
   * @return The converted event value
   */
  private Object convertValue(String field, String value) {
    if (value == null) return EMTPY_VALUE;
    if (METADATA_DURATION.equals(field)) {
      return convertDuration(TimeUnit.MILLISECONDS.toNanos(Long.parseLong(value)));
    }
    else {
      Double doubleValue = Doubles.tryParse(value);
      return (doubleValue == null) ? value : doubleValue;
    }
  }

  /**
   * Returns a new {@link InfluxDBEventReporter.Builder} for {@link InfluxDBEventReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
   * @return InfluxDBEventReporter builder
   * @deprecated this method is bugged. Use {@link InfluxDBEventReporter.Factory#forContext} instead.
   */
  @Deprecated
  public static Builder<? extends Builder> forContext(MetricContext context) {
    return new BuilderImpl(context);
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {

    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static class Factory {
    /**
     * Returns a new {@link InfluxDBEventReporter.Builder} for {@link InfluxDBEventReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
     * @return InfluxDBEventReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  /**
   * Builder for {@link InfluxDBEventReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds using TCP connection
   */
  public static abstract class Builder<T extends EventReporter.Builder<T>>
      extends EventReporter.Builder<T> {
    protected String url;
    protected String username;
    protected String password;
    protected String database;
    protected InfluxDBConnectionType connectionType;
    protected Optional<InfluxDBPusher> influxDBPusher;

    protected Builder(MetricContext context) {
      super(context);
      this.influxDBPusher = Optional.absent();
      this.connectionType = InfluxDBConnectionType.TCP;
    }

    /**
     * Set {@link org.apache.gobblin.metrics.influxdb.InfluxDBPusher} to use.
     */
    public T withInfluxDBPusher(InfluxDBPusher pusher) {
      this.influxDBPusher = Optional.of(pusher);
      return self();
    }

    /**
     * Set connection parameters for the {@link org.apache.gobblin.metrics.influxdb.InfluxDBPusher} creation
     */
    public T withConnection(String url, String username, String password, String database) {
      this.url = url;
      this.username = username;
      this.password = password;
      this.database = database;
      return self();
    }

    /**
     * Set {@link org.apache.gobblin.metrics.influxdb.InfluxDBConnectionType} to use.
     */
    public T withConnectionType(InfluxDBConnectionType connectionType) {
      this.connectionType = connectionType;
      return self();
    }

    /**
     * Builds and returns {@link InfluxDBEventReporter}.
     *
     * @return InfluxDBEventReporter
     */
    public InfluxDBEventReporter build() throws IOException {
      return new InfluxDBEventReporter(this);
    }
  }

  private Point buildEventAsPoint(String name, Object value, long timestamp) throws IOException {
    return Point.measurement(name).field("value", value).time(timestamp, TimeUnit.MILLISECONDS).build();
  }

}
