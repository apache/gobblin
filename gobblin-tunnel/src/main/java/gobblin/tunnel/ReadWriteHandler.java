/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.tunnel;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

import static java.nio.channels.SelectionKey.OP_READ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the relaying of data back and forth between the Client-to-Tunnel and Tunnel-to-Proxy
 * socket connections. This class is not thread safe.
 */
class ReadWriteHandler implements Callable<HandlerState> {
  static final Logger LOG = LoggerFactory.getLogger(Tunnel.class);
  private final SocketChannel proxy;
  private final SocketChannel client;
  private final Selector selector;
  private final ByteBuffer buffer = ByteBuffer.allocate(1000000);
  private HandlerState state = HandlerState.READING;

  ReadWriteHandler(SocketChannel proxy, ByteBuffer mixedServerResponseBuffer, SocketChannel client, Selector selector)
      throws IOException {
    this.proxy = proxy;
    this.client = client;
    this.selector = selector;

    // drain response that is not part of proxy's 200 OK and is part of data pushed from server, and push to client
    if (mixedServerResponseBuffer.limit() > mixedServerResponseBuffer.position()) {
      this.client.configureBlocking(true);
      OutputStream clientOut = this.client.socket().getOutputStream();
      clientOut.write(mixedServerResponseBuffer.array(), mixedServerResponseBuffer.position(),
          mixedServerResponseBuffer.limit() - mixedServerResponseBuffer.position());
      clientOut.flush();
    }
    this.proxy.configureBlocking(false);
    this.client.configureBlocking(false);

    this.client.register(this.selector, OP_READ, this);
    this.proxy.register(this.selector, OP_READ, this);
  }

  @Override
  public HandlerState call()
      throws Exception {
    try {
      switch (state){
        case READING:
          read();
          break;
        case WRITING:
          write();
          break;
      }
    } catch (IOException ioe) {
      closeChannels();
      throw new IOException(String.format("Could not read/write between %s and %s", proxy, client), ioe);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      try {
        closeChannels();
      } finally {
        throw e;
      }
    }
    return state;
  }

  private void write()
      throws IOException {
    SelectionKey proxyKey = proxy.keyFor(selector);
    SelectionKey clientKey = client.keyFor(selector);

    SocketChannel writeChannel = null;
    SocketChannel readChannel = null;
    SelectionKey writeKey = null;

    if (selector.selectedKeys().contains(proxyKey) && proxyKey.isValid() && proxyKey.isWritable()) {
      writeChannel = proxy;
      readChannel = client;
      writeKey = proxyKey;
    } else if (selector.selectedKeys().contains(clientKey) && clientKey.isValid() && clientKey.isWritable()) {
      writeChannel = client;
      readChannel = proxy;
      writeKey = clientKey;
    }

    if (writeKey != null) {
      int lastWrite, totalWrite = 0;

      buffer.flip();

      int available = buffer.remaining();

      while ((lastWrite = writeChannel.write(buffer)) > 0) {
        totalWrite += lastWrite;
      }

      LOG.debug("{} bytes written to {}", totalWrite, writeChannel == proxy ? "proxy" : "client");

      if (totalWrite == available) {
        buffer.clear();
        if(readChannel.isOpen()) {
          readChannel.register(selector, SelectionKey.OP_READ, this);
          writeChannel.register(selector, SelectionKey.OP_READ, this);
        }
        else{
          writeChannel.close();
        }
        state = HandlerState.READING;
      } else {
        buffer.compact();
      }
      if (lastWrite == -1) {
        closeChannels();
      }
    }
  }

  private void read()
      throws IOException {
    SelectionKey proxyKey = proxy.keyFor(selector);
    SelectionKey clientKey = client.keyFor(selector);

    SocketChannel readChannel = null;
    SocketChannel writeChannel = null;
    SelectionKey readKey = null;

    if (selector.selectedKeys().contains(proxyKey) && proxyKey.isReadable()) {
      readChannel = proxy;
      writeChannel = client;
      readKey = proxyKey;
    } else if (selector.selectedKeys().contains(clientKey) && clientKey.isReadable()) {
      readChannel = client;
      writeChannel = proxy;
      readKey = clientKey;
    }

    if (readKey != null) {

      int lastRead, totalRead = 0;

      while ((lastRead = readChannel.read(buffer)) > 0) {
        totalRead += lastRead;
      }

      LOG.debug("{} bytes read from {}", totalRead, readChannel == proxy ? "proxy":"client");

      if (totalRead > 0) {
        readKey.cancel();
        writeChannel.register(selector, SelectionKey.OP_WRITE, this);
        state = HandlerState.WRITING;
      }
      if (lastRead == -1) {
        readChannel.close();
      }
    }
  }

  private void closeChannels() {
    if (proxy.isOpen()) {
      try {
        proxy.close();
      } catch (IOException log) {
        LOG.warn("Failed to close proxy channel {}", proxy,log);
      }
    }

    if (client.isOpen()) {
      try {
        client.close();
      } catch (IOException log) {
        LOG.warn("Failed to close client channel {}", client,log);
      }
    }
  }
}
