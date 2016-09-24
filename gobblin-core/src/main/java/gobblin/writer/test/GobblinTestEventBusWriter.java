/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.writer.test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.WriterUtils;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

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

  public GobblinTestEventBusWriter(EventBus eventBus) {
    _eventBus = eventBus;
  }

  public GobblinTestEventBusWriter(String eventBusId) {
    this(TestingEventBuses.getEventBus(eventBusId));
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
  }

  @Override
  public void write(Object record) throws IOException {
    _eventBus.post(new TestingEventBuses.Event(record));
    _recordCount.incrementAndGet();
  }

  @Override
  public void commit() throws IOException {
    // Nothing to do
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
      return WriterUtils.getWriterOutputDir(getDestination().getProperties(),
                                            getBranches(),
                                            getBranch())
                        .toString();
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

    @Override public GobblinTestEventBusWriter build() throws IOException {
      return new GobblinTestEventBusWriter(getEventBusId());
    }

  }

}
