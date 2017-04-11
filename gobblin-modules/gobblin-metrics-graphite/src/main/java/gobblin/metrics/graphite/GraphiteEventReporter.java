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

package gobblin.metrics.graphite;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.MultiPartEvent;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.JobEvent;
import gobblin.metrics.event.TaskEvent;
import gobblin.metrics.reporter.EventReporter;

import static gobblin.metrics.event.TimingEvent.METADATA_DURATION;


/**
 *
 * {@link gobblin.metrics.reporter.EventReporter} that emits {@link gobblin.metrics.GobblinTrackingEvent} events
 * as timestamped name - value pairs through the Graphite protocol
 *
 * @author Lorand Bendig
 *
 */
public class GraphiteEventReporter extends EventReporter {

  private final GraphitePusher graphitePusher;
  private final boolean emitValueAsKey;

  private static final String EMTPY_VALUE = "0";
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteEventReporter.class);

  public GraphiteEventReporter(Builder<?> builder) throws IOException {
    super(builder);
    if (builder.graphitePusher.isPresent()) {
      this.graphitePusher = builder.graphitePusher.get();
    } else {
      this.graphitePusher =
          this.closer.register(new GraphitePusher(builder.hostname, builder.port, builder.connectionType));
    }
    this.emitValueAsKey = builder.emitValueAsKey;
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {

    GobblinTrackingEvent nextEvent;
    try {
      while (null != (nextEvent = queue.poll())) {
        pushEvent(nextEvent);
      }
      this.graphitePusher.flush();
    } catch (IOException e) {
      LOGGER.error("Error sending event to Graphite", e);
      try {
        this.graphitePusher.flush();
      } catch (IOException e1) {
        LOGGER.error("Unable to flush previous events to Graphite", e);
      }
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
    long timestamp = event.getTimestamp() / 1000l;
    MultiPartEvent multipartEvent = MultiPartEvent.getEvent(metadata.get(EventSubmitter.EVENT_TYPE));
    if (multipartEvent == null) {
      graphitePusher.push(name, EMTPY_VALUE, timestamp);
    }
    else {
      for (String field : multipartEvent.getMetadataFields()) {
        String value = metadata.get(field);
        if (value == null) {
          graphitePusher.push(JOINER.join(name, field), EMTPY_VALUE, timestamp);
        } else {
          if (emitAsKey(field)) {
            // metric value is emitted as part of the keys
            graphitePusher.push(JOINER.join(name, field, value), EMTPY_VALUE, timestamp);
          } else {
            graphitePusher.push(JOINER.join(name, field), convertValue(field, value), timestamp);
          }
        }
      }
    }
  }

  private String convertValue(String field, String value) {
    return METADATA_DURATION.equals(field) ?
        Double.toString(convertDuration(TimeUnit.MILLISECONDS.toNanos(Long.parseLong(value)))) : value;
  }

  /**
   * Non-numeric event values may be emitted as part of the key by applying them to the end of the key if
   * {@link ConfigurationKeys#METRICS_REPORTING_GRAPHITE_EVENT_VALUE_AS_KEY} is set. Thus such events can be still
   * reported even when the backend doesn't accept text values through Graphite
   *
   * @param field name of the metric's metadata fields
   * @return true if event value is emitted in the key
   */
  private boolean emitAsKey(String field) {
    return emitValueAsKey
        && (field.equals(TaskEvent.METADATA_TASK_WORKING_STATE) || field.equals(JobEvent.METADATA_JOB_STATE));
  }

  /**
   * Returns a new {@link GraphiteEventReporter.Builder} for {@link GraphiteEventReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return GraphiteEventReporter builder
   * @deprecated this method is bugged. Use {@link GraphiteEventReporter.Factory#forContext} instead.
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
     * Returns a new {@link GraphiteEventReporter.Builder} for {@link GraphiteEventReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link gobblin.metrics.MetricContext} to report
     * @return GraphiteEventReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  /**
   * Builder for {@link GraphiteEventReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds using TCP connection
   */
  public static abstract class Builder<T extends EventReporter.Builder<T>> extends EventReporter.Builder<T> {
    protected String hostname;
    protected int port;
    protected GraphiteConnectionType connectionType;
    protected Optional<GraphitePusher> graphitePusher;
    protected boolean emitValueAsKey;

    protected Builder(MetricContext context) {
      super(context);
      this.graphitePusher = Optional.absent();
      this.connectionType = GraphiteConnectionType.TCP;
    }

    /**
     * Set {@link gobblin.metrics.graphite.GraphitePusher} to use.
     */
    public T withGraphitePusher(GraphitePusher pusher) {
      this.graphitePusher = Optional.of(pusher);
      return self();
    }

    /**
     * Set connection parameters for the {@link gobblin.metrics.graphite.GraphitePusher} creation
     */
    public T withConnection(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
      return self();
    }

    /**
     * Set {@link gobblin.metrics.graphite.GraphiteConnectionType} to use.
     */
    public T withConnectionType(GraphiteConnectionType connectionType) {
      this.connectionType = connectionType;
      return self();
    }

    /**
     * Set flag that forces the reporter to emit non-numeric event values as part of the key
     */
    public T withEmitValueAsKey(boolean emitValueAsKey) {
      this.emitValueAsKey = emitValueAsKey;
      return self();
    }

    /**
     * Builds and returns {@link GraphiteEventReporter}.
     *
     * @return GraphiteEventReporter
     */
    public GraphiteEventReporter build() throws IOException {
      return new GraphiteEventReporter(this);
    }
  }

}
