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

package gobblin.tunnel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpForward;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for HTTP traffic through Tunnel via an HTTP proxy.
 *
 * @author navteniev@linkedin.com
 */
@Test(groups = { "gobblin.tunnel", "disabledOnTravis" })
public class TunnelTest {

  private ClientAndServer mockServer;
  int PORT = 0;

  @BeforeClass
  public void startProxy()
      throws IOException {
    mockServer = ClientAndServer.startClientAndServer(0);
    PORT = mockServer.getPort();
  }

  @AfterClass
  public void stopProxy() {
    mockServer.stop();
  }

  @AfterMethod
  public void reset(){
    mockServer.reset();
  }

  @Test
  public void mustBuildTunnelAndStartAcceptingConnections()
      throws Exception {
    Tunnel tunnel = Tunnel.build("example.org", 80, "localhost", PORT);

    try {
      int tunnelPort = tunnel.getPort();
      assertTrue(tunnelPort > 0);
    } finally {
      tunnel.close();
    }
  }

  @Test
  public void mustHandleClientDisconnectingWithoutClosingTunnel()
      throws Exception {
    mockExample();
    Tunnel tunnel = Tunnel.build("example.org", 80, "localhost", PORT);

    try {
      int tunnelPort = tunnel.getPort();
      SocketChannel client = SocketChannel.open();

      client.connect(new InetSocketAddress("localhost", tunnelPort));
      client.write(ByteBuffer.wrap("GET / HTTP/1.1%nUser-Agent: GobblinTunnel%nConnection:keep - alive %n%n".getBytes()));
      client.close();

      assertNotNull(fetchContent(tunnelPort));
    } finally {
      tunnel.close();
    }
  }

  @Test
  public void mustHandleConnectionToExternalResource()
      throws Exception {

    mockExample();
    Tunnel tunnel = Tunnel.build("example.org", 80, "localhost", PORT);

    try {
      String content = fetchContent(tunnel.getPort());

      assertNotNull(content);
    } finally {
      tunnel.close();
    }
  }

  @Test
  public void mustHandleMultipleConnections()
      throws Exception {
    mockExample();
    Tunnel tunnel = Tunnel.build("example.org", 80, "localhost", PORT);
    int clients = 5;

    final CountDownLatch startSignal = new CountDownLatch(1);
    final CountDownLatch doneSignal = new CountDownLatch(clients);

    ExecutorService executor = Executors.newFixedThreadPool(clients);
    try {
      final int tunnelPort = tunnel.getPort();

      List<Future<String>> results = new ArrayList<Future<String>>();

      for (int i = 0; i < clients; i++) {
        Future<String> result = executor.submit(new Callable<String>() {
          @Override
          public String call()
              throws Exception {
            startSignal.await();

            try {
              return fetchContent(tunnelPort);
            } finally {
              doneSignal.countDown();
            }
          }
        });

        results.add(result);
      }

      startSignal.countDown();
      doneSignal.await();

      for (Future<String> result : results) {
        assertNotNull(result.get());
      }
    } finally {
      tunnel.close();
    }
  }

  @Test(expectedExceptions = SocketException.class)
  public void mustRefuseConnectionWhenProxyIsUnreachable()
      throws Exception {

    Tunnel tunnel = Tunnel.build("example.org", 80, "localhost", 1);

    try {
      int tunnelPort = tunnel.getPort();

      fetchContent(tunnelPort);
    } finally {
      tunnel.close();
    }
  }

  @Test(expectedExceptions = SocketException.class)
  public void mustRefuseConnectionWhenProxyRefuses() throws Exception{
    mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("www.us.apache.org:80"))
        .respond(HttpResponse.response().withStatusCode(403));

    Tunnel tunnel = Tunnel.build("example.org", 80, "localhost", PORT);

    try {
      int tunnelPort = tunnel.getPort();

      fetchContent(tunnelPort);
    } finally {
      tunnel.close();
    }
  }

  @Test(expectedExceptions = SocketException.class)
  public void mustRefuseConnectionWhenProxyTimesOut() throws Exception{
    mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("www.us.apache.org:80"))
        .respond(HttpResponse.response().withDelay(TimeUnit.SECONDS,2).withStatusCode(200));

    Tunnel tunnel = Tunnel.build("example.org", 80, "localhost", PORT);

    try {
      int tunnelPort = tunnel.getPort();

      fetchContent(tunnelPort);
    } finally {
      tunnel.close();
    }
  }

  @Test(enabled = false)
  public void mustDownloadLargeFiles()
      throws Exception {

    mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("www.us.apache.org:80"))
        .respond(HttpResponse.response().withStatusCode(200));
    mockServer.when(HttpRequest.request().withMethod("GET")
        .withPath("/dist//httpcomponents/httpclient/binary/httpcomponents-client-4.5.1-bin.tar.gz"))
        .forward(HttpForward.forward().withHost("www.us.apache.org").withPort(80));

    Tunnel tunnel = Tunnel.build("www.us.apache.org", 80, "localhost", PORT);
    try {
      IOUtils.copyLarge((InputStream) new URL("http://localhost:" + tunnel.getPort()
              + "/dist//httpcomponents/httpclient/binary/httpcomponents-client-4.5.1-bin.tar.gz")
              .getContent(new Class[]{InputStream.class}),
          new FileOutputStream(File.createTempFile("httpcomponents-client-4.5.1-bin", "tar.gz")));
    } finally {
      tunnel.close();
    }
  }

  private String fetchContent(int tunnelPort)
      throws IOException {
    InputStream content = (InputStream) new URL(String.format("http://localhost:%s/", tunnelPort)).openConnection()
        .getContent(new Class[]{InputStream.class});
    return IOUtils.toString(content);
  }

  private void mockExample()
      throws IOException {
    mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("example.org:80"))
        .respond(HttpResponse.response().withStatusCode(200));
    mockServer.when(HttpRequest.request().withMethod("GET").withPath("/"))
        .respond(HttpResponse.response(IOUtils.toString(getClass().getResourceAsStream("/example.org.html"))));
  }

}
