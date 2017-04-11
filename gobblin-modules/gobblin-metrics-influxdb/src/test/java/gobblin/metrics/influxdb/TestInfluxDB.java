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

package gobblin.metrics.influxdb;

import gobblin.metrics.test.TimestampedValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import com.google.common.collect.Maps;


/**
 * A test implementation of {@link org.influxdb.InfluxDB}.
 *
 * @author Lorand Bendig
 *
 */
public class TestInfluxDB implements InfluxDB {

  private final Map<String, TimestampedValue> data = Maps.newHashMap();

  @Override
  public InfluxDB setLogLevel(LogLevel logLevel) {
    // Nothing to do
    return this;
  }

  @Override
  public InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit) {
    // Nothing to do
    return this;
  }

  @Override
  public void disableBatch() {
    // Nothing to do
  }

  @Override
  public Pong ping() {
    // Nothing to do
    return null;
  }

  @Override
  public String version() {
    // Nothing to do
    return null;
  }

  @Override
  public void write(String database, String retentionPolicy, Point point) {
    BatchPoints batchPoints = BatchPoints.database(database).retentionPolicy(retentionPolicy).build();
    batchPoints.point(point);
    this.write(batchPoints);
  }

  @Override
  public void write(BatchPoints batchPoints) {
    for (Point point : batchPoints.getPoints()) {
      write(point.lineProtocol());
    }
  }

  @Override
  public QueryResult query(Query query) {
    // Nothing to do
    return null;
  }

  @Override
  public QueryResult query(Query query, TimeUnit timeUnit) {
    // Nothing to do
    return null;
  }

  @Override
  public void createDatabase(String name) {
    // Nothing to do
  }

  @Override
  public void deleteDatabase(String name) {
    // Nothing to do
  }

  @Override
  public List<String> describeDatabases() {
    // Nothing to do
    return null;
  }

  @Override
  public void setConnectTimeout(long connectTimeout, TimeUnit timeUnit) {
    // Nothing to do
  }

  @Override
  public void setReadTimeout(long readTimeout, TimeUnit timeUnit) {
    // Nothing to do
  }

  @Override
  public void setWriteTimeout(long writeTimeout, TimeUnit timeUnit) {
    // Nothing to do
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

  private void write(String lineProtocol) {
    String[] split = lineProtocol.split(" ");
    String key = split[0];
    String value = split[1].substring(split[1].indexOf('=') + 1, split[1].length());
    long timestamp = Long.valueOf(split[2]) / 1000000l;
    data.put(key, new TimestampedValue(timestamp, value));
  }

}
