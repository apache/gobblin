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
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for accepting connections from the client on the socket the Tunnel listens on.
 */
class AcceptHandler implements Callable<HandlerState> {
  private static final Logger LOG = LoggerFactory.getLogger(Tunnel.class);

  private final ServerSocketChannel server;
  private final Selector selector;
  private final Config config;

  AcceptHandler(ServerSocketChannel server, Selector selector, Config config) {
    this.config = config;
    this.server = server;
    this.selector = selector;
  }

  @Override
  public HandlerState call()
      throws Exception {
    SocketChannel client = this.server.accept();

    LOG.info("Accepted connection from {}", client.getRemoteAddress());
    try {
      new ProxySetupHandler(client, selector, config);
    } catch (IOException ioe) {
      client.close();
    }
    return HandlerState.ACCEPTING;
  }
}
