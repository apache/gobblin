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
package gobblin.writer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;

/**
 * The purpose of this class is to add more feature to DataWriter such as retry or throttle.
 * Note that RetryWriter will be always applied.
 */
public class DataWriterWrapperBuilder<D> extends DataWriterBuilder<Void, D> {
  private static final Logger LOG = LoggerFactory.getLogger(DataWriterWrapperBuilder.class);

  private final DataWriter<D> writer;
  private final State state;

  public DataWriterWrapperBuilder(DataWriter<D> writer, State state) {
    this.writer = writer;
    this.state = state;
  }

  /**
   * Build the writer with adding throttling (if requested), and retrying feature on top of the writer.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriterBuilder#build()
   */
  @Override
  public DataWriter<D> build() throws IOException {

    DataWriter<D> wrapped = writer;
    if (state.contains(ThrottleWriter.WRITER_LIMIT_RATE_LIMIT_KEY)
        && state.contains(ThrottleWriter.WRITER_THROTTLE_TYPE_KEY)) {
      wrapped = new ThrottleWriter<>(wrapped, state);
    }
    wrapped = new RetryWriter<>(wrapped, state);
    return wrapped;
  }
}
