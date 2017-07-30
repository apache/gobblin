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

import gobblin.metrics.test.TimestampedValue;

import java.io.IOException;
import java.util.Map;

import com.codahale.metrics.graphite.GraphiteSender;

import com.google.common.collect.Maps;


/**
 * A test implementation of {@link com.codahale.metrics.graphite.GraphiteSender}.
 *
 * @author Yinan Li
 */
public class TestGraphiteSender implements GraphiteSender {

  private final Map<String, TimestampedValue> data = Maps.newHashMap();

  @Override
  public void connect() throws IllegalStateException, IOException {
    // Nothing to do
  }

  @Override
  public void send(String name, String value, long timestamp) throws IOException {
    this.data.put(name, new TimestampedValue(timestamp, value));
  }

  @Override
  public void flush() throws IOException {
    // Nothing to do
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public int getFailures() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    this.data.clear();
  }

  /**
   * Get a metric with a given name.
   *
   * @param name metric name
   * @return a {@link gobblin.metrics.TimestampedValue}
   */
  public TimestampedValue getMetric(String name) {
    return this.data.get(name);
  }

}
