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

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Due to the lack of a suitable embeddable proxy server (the Jetty version here is too old and MockServer's Proxy
 * expects SSL traffic and breaks for arbitrary bytes) we had to write our own mini CONNECT proxy. This simply gets
 * an HTTP CONNECT request over a socket, opens another socket to the specified remote server and relays bytes between
 * the two connections.
 *
 * @author kkandekar@linkedin.com
 */
class ConnectProxyServer extends MockServer {
  private final boolean mixServerAndProxyResponse;
  private final boolean largeResponse;
  Pattern hostPortPattern;
  int nBytesToCloseSocketAfter;

  /**
   * @param mixServerAndProxyResponse Force proxy to send 200 OK and server response in single write such that both
   *                                  responses reach the tunnel in the same read. This can happen for a multitude of
   *                                  reasons, e.g. the proxy GC's, or the network hiccups, or the tunnel GC's.
   * @param largeResponse Force proxy to send a large response
   * @param nBytesToCloseSocketAfter
   */
  public ConnectProxyServer(boolean mixServerAndProxyResponse, boolean largeResponse, int nBytesToCloseSocketAfter) {
    this.mixServerAndProxyResponse = mixServerAndProxyResponse;
    this.largeResponse = largeResponse;
    this.nBytesToCloseSocketAfter = nBytesToCloseSocketAfter;
    hostPortPattern = Pattern.compile("Host: (.*):([0-9]+)");
  }

  @Override
  void handleClientSocket(Socket clientSocket) throws IOException {
    final InputStream clientToProxyIn = clientSocket.getInputStream();
    BufferedReader clientToProxyReader = new BufferedReader(new InputStreamReader(clientToProxyIn));
    final OutputStream clientToProxyOut = clientSocket.getOutputStream();
    String line = clientToProxyReader.readLine();
    String connectRequest = "";
    while (line != null && isServerRunning()) {
      connectRequest += line + "\r\n";
      if (connectRequest.endsWith("\r\n\r\n")) {
        break;
      }
      line = clientToProxyReader.readLine();
    }
    // connect to given host:port
    Matcher matcher = hostPortPattern.matcher(connectRequest);
    if (!matcher.find()) {
      try {
        sendConnectResponse("400 Bad Request", clientToProxyOut, null, 0);
      } finally {
        clientSocket.close();
        stopServer();
      }
      return;
    }
    String host = matcher.group(1);
    int port = Integer.decode(matcher.group(2));

    // connect to server
    Socket serverSocket = new Socket();
    try {
      serverSocket.connect(new InetSocketAddress(host, port));
      addSocket(serverSocket);
      byte [] initialServerResponse = null;
      int nbytes = 0;
      if (mixServerAndProxyResponse) {
        // we want to mix the initial server response with the 200 OK
        initialServerResponse = new byte[64];
        nbytes = serverSocket.getInputStream().read(initialServerResponse);
      }
      sendConnectResponse("200 OK", clientToProxyOut, initialServerResponse, nbytes);
    } catch (IOException e) {
      try {
        sendConnectResponse("404 Not Found", clientToProxyOut, null, 0);
      } finally {
        clientSocket.close();
        stopServer();
      }
      return;
    }
    final InputStream proxyToServerIn = serverSocket.getInputStream();
    final OutputStream proxyToServerOut = serverSocket.getOutputStream();
    _threads.add(new EasyThread() {
      @Override
      void runQuietly() throws Exception {
        try {
          IOUtils.copy(clientToProxyIn, proxyToServerOut);
        } catch (IOException e) {
          LOG.warn("Exception " + e.getMessage() + " on " + getServerSocketPort());
        }
      }
    }.startThread());
    try {
      if (nBytesToCloseSocketAfter > 0) {
        // Simulate proxy abruptly closing connection
        int leftToRead = nBytesToCloseSocketAfter;
        byte [] buffer = new byte[leftToRead+ 256];
        while (true) {
          int numRead = proxyToServerIn.read(buffer, 0, leftToRead);
          if (numRead < 0) {
            break;
          }
          clientToProxyOut.write(buffer, 0, numRead);
          clientToProxyOut.flush();
          leftToRead -= numRead;
          if (leftToRead <= 0) {
            LOG.warn("Cutting connection after " + nBytesToCloseSocketAfter + " bytes");
            break;
          }
        }
      } else {
        IOUtils.copy(proxyToServerIn, clientToProxyOut);
      }
    } catch (IOException e) {
      LOG.warn("Exception " + e.getMessage() + " on " + getServerSocketPort());
    }
    clientSocket.close();
    serverSocket.close();
  }

  private void sendConnectResponse(String statusMessage, OutputStream out,
                                   byte[] initialServerResponse, int initialServerResponseSize)
      throws IOException {
    String extraHeader = "";
    if (largeResponse) {
      // this is to force multiple reads while draining the proxy CONNECT response in Tunnel. Normal proxy responses
      // won't be this big (well, unless you annoy squid proxy, which happens sometimes), but a select() call
      // waking up for multiple reads before a buffer is full is normal
      for (int i = 0; i < 260; i++) {
        extraHeader += "a";
      }
    }
    byte [] httpResponse = ("HTTP/1.1 " + statusMessage + "\r\nContent-Length: 0\r\nServer: MockProxy" + extraHeader + "\r\n\r\n").getBytes();
    if (initialServerResponse != null) {
      byte [] mixedResponse = new byte[httpResponse.length + initialServerResponseSize];
      System.arraycopy(httpResponse, 0, mixedResponse, 0, httpResponse.length);
      System.arraycopy(initialServerResponse, 0, mixedResponse, httpResponse.length, initialServerResponseSize);
      out.write(mixedResponse);
    } else {
      out.write(httpResponse);
    }
    out.flush();
  }
}
