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

package org.apache.gobblin.couchbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;

import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.google.common.base.Throwables;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.TestUtils;


@Slf4j
public class CouchbaseTestServer {

  private static final String COUCHBASE_JAR_PATH="gobblin-modules/gobblin-couchbase/mock-couchbase/target/";
  private static final String COUCHBASE_MOCK_JAR=COUCHBASE_JAR_PATH + "CouchbaseMock-1.4.4.jar";


  private Process couchbaseProcess;
  private int _port;
  private int _serverPort;

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
      "org.couchbase.mock.CouchbaseMock",
      "--port",
      _port +"",
      "-n",
      "1",
      "-R",
      "0",
      "-b",
      "default:",
      "--host",
      "127.0.0.1"};

    try {
      System.out.println("Will run command " + Arrays.toString(commands));
      couchbaseProcess = new ProcessBuilder().inheritIO().command(commands).start();
    }
    catch (Exception e)
    {
      log.error("Failed to start couchbase mock server", e);
      Throwables.propagate(e);
    }

    boolean isUp = false;
    int numTries = 5;
    while (!isUp && numTries-- > 0)
    {
      try {
        Thread.sleep(500); // wait .5 secs
        isUp = isUp();
      }
      catch (Exception e)
      {

      }
    }
    Assert.assertTrue(isUp, "Server is not up!");
    fillServerPort();
  }


  public boolean isUp()
  {
    try {
      URL url = new URL("http://localhost:" + _port + "/pools");
      HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
      int responseCode = httpURLConnection.getResponseCode();
      return true;
    }
    catch (Exception e) {
      Throwables.propagate(e);
      return false;
    }
  }

  private void fillServerPort()
  {
    try {
      URL url = new URL("http://localhost:" + _port + "/pools/default/buckets");
      HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
      httpURLConnection.setRequestProperty("Accept", "application/json");
      httpURLConnection.setRequestMethod("GET");

      if (200 <= httpURLConnection.getResponseCode() && httpURLConnection.getResponseCode() <= 299) {
        BufferedReader br = new BufferedReader(new InputStreamReader((httpURLConnection.getInputStream())));
        StringBuilder sb = new StringBuilder();
        String output;
        while ((output = br.readLine()) != null) {
          sb.append(output);
        }
        JSONArray json = new JSONArray(sb.toString());
        log.debug(json.toString());
        int serverPort =
            (Integer) ((JSONObject) ((JSONObject) ((JSONArray) ((JSONObject) json.get(0)).get("nodes")).get(0)).get("ports")).get("direct");
        _serverPort = serverPort;
      }
    }
      catch (Exception e) {
        log.error("Server is not up", e);
        Throwables.propagate(e);
      }
  }

  public int getServerPort() {
    return _serverPort;
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


  @Test
  public static void testServer()
      throws InterruptedException, IOException {
    CouchbaseTestServer couchbaseTestServer = new CouchbaseTestServer(TestUtils.findFreePort());
    couchbaseTestServer.start();

    int port = couchbaseTestServer.getPort();
    int serverPort = couchbaseTestServer.getServerPort();



    try {
      CouchbaseEnvironment cbEnv = DefaultCouchbaseEnvironment.builder().bootstrapHttpEnabled(true)
          .bootstrapHttpDirectPort(port)
          .bootstrapCarrierDirectPort(serverPort)
          .connectTimeout(TimeUnit.SECONDS.toMillis(15))
          .bootstrapCarrierEnabled(true).build();
      CouchbaseCluster cbCluster = CouchbaseCluster.create(cbEnv, "localhost");
      Bucket bucket = cbCluster.openBucket("default","");
      try {
        JsonObject content = JsonObject.empty().put("name", "Michael");
        JsonDocument doc = JsonDocument.create("docId", content);
        JsonDocument inserted = bucket.insert(doc);
      }
      catch (Exception e)
      {
        Assert.fail("Should not throw exception on insert", e);
      }
    }
    finally
    {
      couchbaseTestServer.stop();
    }
  }
}
