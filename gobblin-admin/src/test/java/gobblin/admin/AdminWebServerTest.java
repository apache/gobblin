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
package gobblin.admin;

import gobblin.configuration.ConfigurationKeys;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.testng.annotations.*;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import static org.testng.AssertJUnit.*;


public class AdminWebServerTest {
  private AdminWebServer server;
  private final String portNumber = "54320";

  @BeforeTest
  public void startServer() {
    Properties properties = new Properties();
    properties.put(ConfigurationKeys.ADMIN_SERVER_PORT_KEY, this.portNumber);
    this.server = new AdminWebServer(properties, URI.create("http://foobar:3333"));

    try {
      this.server.startUp();
    } catch (Exception e) {
      fail(String.format("Exception starting server: %s", e.toString()));
    }
  }

  @AfterTest
  public void stopServer() {
    try {
      this.server.shutDown();
    } catch (Exception e) {
      // do nothing
    }
  }

  @Test
  public void testGetSettingsJs() throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpGet getReq = new HttpGet(String.format("http://localhost:%s/js/settings.js", this.portNumber));

    try (CloseableHttpResponse response = client.execute(getReq)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
      HttpEntity body = response.getEntity();
      String bodyString = EntityUtils.toString(body);
      assertStringContains("http://foobar", bodyString);
      assertStringContains("3333", bodyString);
    }
  }

  @Test
  public void testGetIndex() throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpGet getReq = new HttpGet(String.format("http://localhost:%s/", this.portNumber));

    try (CloseableHttpResponse response = client.execute(getReq)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
      HttpEntity body = response.getEntity();
      String bodyString = EntityUtils.toString(body);
      assertStringContains("JOB SUMMARY", bodyString);
    }
  }

  private static void assertStringContains(String expected, String container) {
    assertTrue(String.format("Expected %s to contain %s", container, expected), container.contains(expected));
  }
}
