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

import java.net.InetSocketAddress;

import com.codahale.metrics.graphite.Graphite;

import gobblin.metrics.graphite.GraphiteReporter;


/**
 * An implementation of {@link ReporterExampleBase} for a {@link GraphiteReporter}.
 *
 * @author ynli
 */
public class GraphiteReporterExample extends ReporterExampleBase {

  public GraphiteReporterExample(int tasks, long totalRecords, String host) {
    super(GraphiteReporter.builder(new Graphite(new InetSocketAddress(host, 2003))), tasks, totalRecords);
  }

  private static void usage() {
    System.err.println(
        "Usage: GraphiteReporterExample "
            + "<number of tasks to spawn> "
            + "<total number of records to process per task> "
            + "<Graphite Carbon server host name>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      usage();
      System.exit(1);
    }

    GraphiteReporterExample graphiteReporterExample = new GraphiteReporterExample(
        Integer.parseInt(args[0]), Long.parseLong(args[1]), args[2]);
    graphiteReporterExample.run();
  }
}
