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
package gobblin.writer.test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import lombok.Data;


/**
 * This class is meant for automated testing of Gobblin job executions. It will write any object it
 * receives to a Guava EventBus . Tests can subscribe to the event bus and monitor what records are
 * being produced.
 *
 * <p>By default, the class will use TestingEventBuses to create an EventBus with name
 * {@link ConfigurationKeys#WRITER_OUTPUT_DIR}.
 *
 * <p>Note that the EventBus instances are static (to simplify the sharing between writer and tests).
 *  It is responsibility of the test to make sure that names of those are unique to avoid cross-
 *  pollution between tests.
 */
public class GobblinTestEventBusWriter implements DataWriter<Object> {
  private final EventBus _eventBus;
  private final AtomicLong _recordCount = new AtomicLong();
  private final Mode _mode;

  private long _firstRecordTimestamp;
  private long _lastRecordTimestamp;

  public enum Mode {
    /** Will post every record to eventbus. */
    POST_RECORDS,
    /** Will count records and post a summary to eventbus at commit time. */
    COUNTING
  }

  /** The topic to use for writing */
  public static final String EVENTBUSID_KEY = "GobblinTestEventBusWriter.eventBusId";
  public static final String MODE_KEY = "GobblinTestEventBusWriter.mode";

  public static final String FULL_EVENTBUSID_KEY =
      ConfigurationKeys.WRITER_PREFIX + "." + EVENTBUSID_KEY;
  public static final String FULL_MODE_KEY = ConfigurationKeys.WRITER_PREFIX + "." + MODE_KEY;

  public GobblinTestEventBusWriter(EventBus eventBus, Mode mode) {
    _eventBus = eventBus;
    _mode = mode;
  }

  public GobblinTestEventBusWriter(String eventBusId, Mode mode) {
    this(TestingEventBuses.getEventBus(eventBusId), mode);
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
  }

  @Override
  public void write(Object record) throws IOException {
    if (_firstRecordTimestamp == 0) {
      _firstRecordTimestamp = System.currentTimeMillis();
    }
    if (this._mode == Mode.POST_RECORDS) {
      _eventBus.post(new TestingEventBuses.Event(record));
    }
    _lastRecordTimestamp = System.currentTimeMillis();
    _recordCount.incrementAndGet();
  }

  @Override
  public void commit() throws IOException {
    if (this._mode == Mode.COUNTING) {
      _eventBus.post(new TestingEventBuses.Event(new RunSummary(_recordCount.get(), _lastRecordTimestamp - _firstRecordTimestamp)));
    }
  }

  @Override
  public void cleanup() throws IOException {
    // Nothing to do
  }

  @Override
  public long recordsWritten() {
    return _recordCount.get();
  }

  @Override
  public long bytesWritten() throws IOException {
    // Not meaningful
    return _recordCount.get();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends DataWriterBuilder<Object, Object> {
    private Optional<String> _eventBusId = Optional.absent();

    public String getDefaultEventBusId() {
      State destinationCfg = getDestination().getProperties();
      String eventBusIdKey =
          ForkOperatorUtils.getPathForBranch(destinationCfg, FULL_EVENTBUSID_KEY, getBranches(),
                                             getBranch());
      if (destinationCfg.contains(eventBusIdKey)) {
        return destinationCfg.getProp(eventBusIdKey);
      }
      else {
        return WriterUtils.getWriterOutputDir(destinationCfg,
                                              getBranches(),
                                              getBranch())
                          .toString();
      }
    }

    public String getEventBusId() {
      if (! _eventBusId.isPresent()) {
        _eventBusId = Optional.of(getDefaultEventBusId());
      }
      return _eventBusId.get();
    }

    public Builder withEventBusId(String eventBusId) {
      _eventBusId = Optional.of(eventBusId);
      return this;
    }

    public Mode getDefaultMode() {
      try {
        State destinationCfg = getDestination().getProperties();
        String modeKey = ForkOperatorUtils.getPathForBranch(destinationCfg, FULL_MODE_KEY, getBranches(), getBranch());

        return Mode.valueOf(destinationCfg.getProp(modeKey, Mode.POST_RECORDS.name()).toUpperCase());
      } catch (Throwable t) {
        return Mode.POST_RECORDS;
      }
    }

    @Override public GobblinTestEventBusWriter build() throws IOException {
      return new GobblinTestEventBusWriter(getEventBusId(), getDefaultMode());
    }

  }

  @Data
  public static class RunSummary {
    private final long recordsWritten;
    private final long timeElapsedMillis;
  }

}
