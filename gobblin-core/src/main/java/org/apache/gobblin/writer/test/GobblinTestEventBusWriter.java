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
package org.apache.gobblin.writer.test;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;


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
  private final AtomicLong _bytesCount = new AtomicLong();
  private final Mode _mode;
  private boolean _isRecordSizeReused = true;
  private long _reusedRecordSize = 0;

  private long _firstRecordTimestamp;
  private long _lastRecordTimestamp;

  public enum Mode {
    /** Will post every record to eventbus. */
    POST_RECORDS,
    /** Will count records and post a summary to eventbus at commit time. */
    COUNTING,
    /** Will count total number of types of records being passed through.
     * Measured by {@link java.lang.instrument.Instrumentation}
     * To use the feature, one needs to add jvm arguments when running java program as below:
     *
     * -javaagent:"/<PATH_TO_YOUR_MULTIPRODUCT>/gobblin-proxy_trunk/gobblin-github/build/gobblin-core/libs/gobblin-core-<VERSION>.jar"
     * */
    POST_BYTES
  }

  /** The topic to use for writing */
  public static final String EVENTBUSID_KEY = "GobblinTestEventBusWriter.eventBusId";
  public static final String MODE_KEY = "GobblinTestEventBusWriter.mode";

  public static final String FULL_EVENTBUSID_KEY =
      ConfigurationKeys.WRITER_PREFIX + "." + EVENTBUSID_KEY;
  public static final String FULL_MODE_KEY = ConfigurationKeys.WRITER_PREFIX + "." + MODE_KEY;

  // Turn this on when there's need to reuse the object size when measuring total amount of bytes being flowing through Gobblin.
  // Most of the case when we do profiling, the fake object should have similar in-memory size.
  public static final String RECORD_SIZE_BEING_REUSED = "GobblinTestEventBusWriter.recordSizeReused";

  public GobblinTestEventBusWriter(EventBus eventBus, Mode mode) {
    _eventBus = eventBus;
    _mode = mode;
  }

  public GobblinTestEventBusWriter(String eventBusId, Mode mode) {
    this(TestingEventBuses.getEventBus(eventBusId), mode);
  }

  public GobblinTestEventBusWriter(State state, String eventBusId, Mode mode) {
    this(TestingEventBuses.getEventBus(eventBusId), mode);
    _isRecordSizeReused = state.getPropAsBoolean(RECORD_SIZE_BEING_REUSED)
        || state.getPropAsBoolean(RECORD_SIZE_BEING_REUSED);
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
  }

  @Override
  public void write(Object record) throws IOException {
    if (_firstRecordTimestamp == 0) {
      _firstRecordTimestamp = System.currentTimeMillis();
      _reusedRecordSize = InstrumentationAgent.getObjectSize(record);
    }
    if (this._mode == Mode.POST_RECORDS || this._mode == Mode.POST_BYTES) {
      _eventBus.post(new TestingEventBuses.Event(record));
    }
    _lastRecordTimestamp = System.currentTimeMillis();
    _recordCount.incrementAndGet();
    _bytesCount.addAndGet(_isRecordSizeReused ? _reusedRecordSize : InstrumentationAgent.getObjectSize(record));
  }

  @Override
  public void commit() throws IOException {
    if (this._mode == Mode.COUNTING) {
      _eventBus.post(new TestingEventBuses.Event(new RunSummary(_recordCount.get(),
          _lastRecordTimestamp - _firstRecordTimestamp, _bytesCount.get())));
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
    return _bytesCount.get();
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
    private final long bytesWritten;
  }

}
