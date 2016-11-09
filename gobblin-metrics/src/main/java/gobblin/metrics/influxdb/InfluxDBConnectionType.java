/*
 * Copyright (C) 2016 Swisscom All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;


/**
 * Connection types used by {@link InfluxDBReporter} and {@link InfluxDBEventReporter}.
 * 
 * @author Lorand Bendig
 *
 */
public enum InfluxDBConnectionType {
  TCP {
    @Override
    public InfluxDB createConnection(String url, String username, String password) {
      return InfluxDBFactory.connect(url, username, password);
    }
  };
  // UDP will be added once InfluxDB-java will support it

  public abstract InfluxDB createConnection(String url, String username, String password);

}
