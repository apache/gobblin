/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase;

import java.io.IOException;

import avro.shaded.com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;

import gobblin.test.TestUtils;


@Slf4j
public class CouchbaseTestServer {

  private static final String COUCHBASE_JAR_PATH="gobblin-modules/gobblin-couchbase/mock-couchbase/target/";
  private static final String COUCHBASE_MOCK_JAR=COUCHBASE_JAR_PATH + "CouchbaseMock-1.*.jar";


  private Process couchbaseProcess;
  private int _port;

  public CouchbaseTestServer(int port)
  {
    _port = port;
  }

  public void start()
  {

    log.info("Starting couchbase server on port " + _port);
    String[] commands = {"/usr/bin/java",
      "-cp",
      COUCHBASE_MOCK_JAR,
      " org.couchbase.mock.CouchbaseMock",
      "--port",
      _port +""};

    try {
      couchbaseProcess = new ProcessBuilder().command(commands).start();
    }
    catch (IOException e)
    {
      log.error("Failed to start couchbase mock server", e);
      Throwables.propagate(e);
    }
  }


  public int getPort() { return _port; }


  public void stop() {

    if (couchbaseProcess != null) {
      try {
        couchbaseProcess.destroy();
      } catch (Exception e) {
        log.warn("Failed to stop the couchbase server", e);
      }
    }
  }


  public static void main(String[] args)
      throws InterruptedException, IOException {
    CouchbaseTestServer couchbaseTestServer = new CouchbaseTestServer(TestUtils.findFreePort());
    couchbaseTestServer.start();
    while (true)
    {
      Thread.sleep(1000);
      System.out.write(".".getBytes());
    }


  }
}
