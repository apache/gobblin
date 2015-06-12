/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.example;

import gobblin.metrics.influxdb.InfluxDBReporter;


/**
 * An implementation of {@link ReporterExampleBase} for a {@link InfluxDBReporter}.
 */
public class InfluxDBReporterExample extends ReporterExampleBase {

  public InfluxDBReporterExample(int tasks, long totalRecords, String url, String user,
      String password, String database) {
    super(InfluxDBReporter.builder().useUrl(url).useUsername(user).userPassword(password).writeTo(database),
        tasks, totalRecords);
  }

  private static void usage() {
    System.err.println(
        "Usage: InfluxDBReporterExample "
            + "<number of tasks to spawn> "
            + "<total number of records to process per task> "
            + "<InfluxDB server URL>"
            + "<InfluxDB user name> "
            + "<InfluxDB password> "
            + "<InfluxDB database to write metrics into>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 6) {
      usage();
      System.exit(1);
    }

    InfluxDBReporterExample influxDBReporterExample = new InfluxDBReporterExample(
        Integer.parseInt(args[0]), Long.parseLong(args[1]), args[2], args[3], args[4], args[5]);
    influxDBReporterExample.run();
  }
}
