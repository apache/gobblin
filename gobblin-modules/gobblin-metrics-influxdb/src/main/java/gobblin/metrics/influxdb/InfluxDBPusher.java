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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;


/**
 * Establishes a connection to InfluxDB and pushes {@link Point}s.
 *
 * @author Lorand Bendig
 *
 */
public class InfluxDBPusher {

  private static final String DEFAULT_RETENTION_POLICY = "default";

  private final InfluxDB influxDB;
  private final String database;

  private InfluxDBPusher(Builder builder) {
    this.influxDB = builder.influxDB;
    this.database = builder.database;
  }

  public static class Builder {
    private final InfluxDB influxDB;
    private final String database;

    public Builder(String url, String username, String password, String database, InfluxDBConnectionType connectionType) {
      this.influxDB = connectionType.createConnection(url, username, password);
      this.database = database;
    }

    /**
     * Set the connection timeout for InfluxDB
     * @param connectTimeout
     * @param timeUnit
     * @return
     */
    public Builder withConnectTimeout(long connectTimeout, TimeUnit timeUnit) {
      influxDB.setConnectTimeout(connectTimeout, timeUnit);
      return this;
    }

    /**
     * Set the writer timeout for the InfluxDB connection
     * @param writeTimeout
     * @param timeUnit
     * @return
     */
    public Builder withWriteTimeout(long writeTimeout, TimeUnit timeUnit) {
      influxDB.setWriteTimeout(writeTimeout, timeUnit);
      return this;
    }

    public InfluxDBPusher build() {
      return new InfluxDBPusher(this);
    }
  }

  /**
   * Push a single Point
   * @param point the {@link Point} to report
   */
  public void push(Point point) {
    BatchPoints.Builder batchPointsBuilder = BatchPoints.database(database).retentionPolicy(DEFAULT_RETENTION_POLICY);
    batchPointsBuilder.point(point);
    influxDB.write(batchPointsBuilder.build());
  }

  /**
   * Push multiple points at once.
   * @param points list of {@link Point}s to report
   */
  public void push(List<Point> points) {
    BatchPoints.Builder batchPointsBuilder = BatchPoints.database(database).retentionPolicy(DEFAULT_RETENTION_POLICY);
    for (Point point : points) {
      batchPointsBuilder.point(point);
    }
    influxDB.write(batchPointsBuilder.build());
  }

}