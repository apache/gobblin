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

package org.apache.gobblin.tunnel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * Handler for setting up the connection from the Tunnel to the intermediate proxy via HTTP CONNECT.
 */
class ProxySetupHandler implements Callable<HandlerState> {
  private static final Logger LOG = LoggerFactory.getLogger(Tunnel.class);

  public static final String HTTP_1_1_OK = "HTTP/1.1 200";
  private static final ByteBuffer OK_REPLY =
      ByteBuffer.wrap(HTTP_1_1_OK.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
  public static final String HTTP_1_0_OK = "HTTP/1.0 200";
  private static final Set<ByteBuffer> OK_REPLIES = new HashSet<>(
      Arrays.asList(OK_REPLY, ByteBuffer.wrap(HTTP_1_0_OK.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING))));

  private final SocketChannel client;
  private final Selector selector;
  private final SocketChannel proxy;
  private HandlerState state = HandlerState.CONNECTING;

  private ByteBuffer buffer;
  private final long connectStartTime;
  private int totalBytesRead = 0;
  private final Config config;

  ProxySetupHandler(SocketChannel client, Selector selector, Config config) throws IOException {
    this.config = config;
    this.client = client;
    this.selector = selector;
    this.buffer =
        ByteBuffer
            .wrap(String
                .format(
                    "CONNECT %s:%d HTTP/1.1\r%nUser-Agent: GobblinTunnel\r%nservice-name: gobblin\r%n"
                        + "Connection: keep-alive\r%nHost: %s:%d\r%n\r%n",
                    config.getRemoteHost(), config.getRemotePort(), config.getRemoteHost(), config.getRemotePort())
                .getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING));

    //Blocking call
    this.proxy = SocketChannel.open();
    this.proxy.configureBlocking(false);
    this.connectStartTime = System.currentTimeMillis();
    boolean connected =
        this.proxy.connect(new InetSocketAddress(this.config.getProxyHost(), this.config.getProxyPort()));

    if (!connected) {
      this.client.configureBlocking(false);
      this.client.register(this.selector, SelectionKey.OP_READ, this);
      this.proxy.register(this.selector, SelectionKey.OP_CONNECT, this);
    } else {
      this.state = HandlerState.WRITING;
      this.proxy.register(this.selector, SelectionKey.OP_WRITE, this);
    }
  }

  @Override
  public HandlerState call() throws Exception {
    try {
      switch (this.state) {
        case CONNECTING:
          connect();
          break;
        case WRITING:
          write();
          break;
        case READING:
          read();
          break;
        default:
          throw new IllegalStateException("ProxySetupHandler should not be in state " + this.state);
      }
    } catch (IOException ioe) {
      LOG.warn("Failed to establish a proxy connection for {}", this.client.getRemoteAddress(), ioe);
      closeChannels();
    }
    return this.state;
  }

  private void connect() throws IOException {
    if (this.proxy.isOpen()) {
      if (this.proxy.finishConnect()) {
        this.proxy.register(this.selector, SelectionKey.OP_WRITE, this);
        SelectionKey clientKey = this.client.keyFor(this.selector);
        if (clientKey != null) {
          clientKey.cancel();
        }
        this.state = HandlerState.WRITING;
      } else if (this.connectStartTime + Config.PROXY_CONNECT_TIMEOUT_MS < System.currentTimeMillis()) {
        LOG.warn("Proxy connect timed out for client {}", this.client);
        closeChannels();
      }
    }
  }

  private void write() throws IOException {
    while (this.proxy.write(this.buffer) > 0) {}

    if (this.buffer.remaining() == 0) {
      this.proxy.register(this.selector, SelectionKey.OP_READ, this);
      this.state = HandlerState.READING;
      this.buffer = ByteBuffer.allocate(1024);
    }
  }

  private void read() throws IOException {
    int lastBytes = 0;

    while ((lastBytes = this.proxy.read(this.buffer)) > 0) {
      // totalBytesRead has to be stateful because read() might return at arbitrary points
      this.totalBytesRead += lastBytes;
    }

    if (this.totalBytesRead >= OK_REPLY.limit()) {
      byte[] temp = this.buffer.array();
      this.buffer.flip();
      if (OK_REPLIES.contains(ByteBuffer.wrap(temp, 0, OK_REPLY.limit()))) {
        // drain the rest of the HTTP response. 2 consecutive CRLFs signify the end of an HTTP
        // message (some proxies return newlines instead of CRLFs)
        for (int i = OK_REPLY.limit(); i <= (this.buffer.limit() - 4); i++) {
          if (((temp[i] == '\n') && (temp[i + 1] == '\n')) || ((temp[i + 1] == '\n') && (temp[i + 2] == '\n'))
              || ((temp[i + 2] == '\n') && (temp[i + 3] == '\n'))
              || ((temp[i] == '\r') && (temp[i + 1] == '\n') && (temp[i + 2] == '\r') && (temp[i + 3] == '\n'))) {
            this.state = null;
            this.buffer.position(i + 4);
            new ReadWriteHandler(this.proxy, this.buffer, this.client, this.selector);
            return;
          }
        }
      } else {
        LOG.error("Got non-200 response from proxy: ["
            + new String(temp, 0, OK_REPLY.limit(), ConfigurationKeys.DEFAULT_CHARSET_ENCODING)
            + "], closing connection.");
        closeChannels();
      }
    }
  }

  private void closeChannels() {
    if (this.proxy.isOpen()) {
      try {
        this.proxy.close();
      } catch (IOException log) {
        LOG.warn("Failed to close proxy channel {}", this.proxy, log);
      }
    }

    if (this.client.isOpen()) {
      try {
        this.client.close();
      } catch (IOException log) {
        LOG.warn("Failed to close client channel {}", this.client, log);
      }
    }
  }
}
