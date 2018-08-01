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
package org.apache.gobblin.elasticsearch;

import java.io.IOException;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * A Test to test that the {@link ElasticsearchTestServer} class does what it is supposed to do
 */
public class ElasticsearchTestServerTest {


  ElasticsearchTestServer _elasticsearchTestServer;

  @BeforeSuite
  public void startServer()
      throws IOException {
    _elasticsearchTestServer = new ElasticsearchTestServer();
    _elasticsearchTestServer.start(60);
  }
  @Test
  public void testServerStart()
      throws InterruptedException, IOException {
      _elasticsearchTestServer.start(60); // second start should be a no-op
  }

  @AfterSuite
  public void stopServer() {
    _elasticsearchTestServer.stop();
  }
}
