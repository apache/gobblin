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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
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
      switch (this.state) {
        case READING:
          read();
          break;
        case WRITING:
          write();
          break;
        default:
          throw new IllegalStateException("ReadWriteHandler should never be in state " + this.state);
      }
    } catch (CancelledKeyException e) {
      LOG.warn("Encountered canceled key while " + this.state, e);
    } catch (IOException ioe) {
      closeChannels();
      throw new IOException(String.format("Could not read/write between %s and %s", this.proxy, this.client), ioe);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      try {
        closeChannels();
      } finally {
        throw e;
      }
    }
    return this.state;
  }

  private void write()
      throws IOException {
    SelectionKey proxyKey = this.proxy.keyFor(this.selector);
    SelectionKey clientKey = this.client.keyFor(this.selector);

    SocketChannel writeChannel = null;
    SocketChannel readChannel = null;
    SelectionKey writeKey = null;

    if (this.selector.selectedKeys().contains(proxyKey) && proxyKey.isValid() && proxyKey.isWritable()) {
      writeChannel = this.proxy;
      readChannel = this.client;
      writeKey = proxyKey;
    } else if (this.selector.selectedKeys().contains(clientKey) && clientKey.isValid() && clientKey.isWritable()) {
      writeChannel = this.client;
      readChannel = this.proxy;
      writeKey = clientKey;
    }

    if (writeKey != null) {
      int lastWrite, totalWrite = 0;

      this.buffer.flip();

      int available = this.buffer.remaining();

      while ((lastWrite = writeChannel.write(this.buffer)) > 0) {
        totalWrite += lastWrite;
      }

      LOG.debug("{} bytes written to {}", totalWrite, writeChannel == this.proxy ? "proxy" : "client");

      if (totalWrite == available) {
        this.buffer.clear();
        if(readChannel.isOpen()) {
          readChannel.register(this.selector, SelectionKey.OP_READ, this);
          writeChannel.register(this.selector, SelectionKey.OP_READ, this);
        }
        else{
          writeChannel.close();
        }
        this.state = HandlerState.READING;
      } else {
        this.buffer.compact();
      }
      if (lastWrite == -1) {
        closeChannels();
      }
    }
  }

  private void read()
      throws IOException {
    SelectionKey proxyKey = this.proxy.keyFor(this.selector);
    SelectionKey clientKey = this.client.keyFor(this.selector);

    SocketChannel readChannel = null;
    SocketChannel writeChannel = null;
    SelectionKey readKey = null;

    if (this.selector.selectedKeys().contains(proxyKey) && proxyKey.isReadable()) {
      readChannel = this.proxy;
      writeChannel = this.client;
      readKey = proxyKey;
    } else if (this.selector.selectedKeys().contains(clientKey) && clientKey.isReadable()) {
      readChannel = this.client;
      writeChannel = this.proxy;
      readKey = clientKey;
    }

    if (readKey != null) {

      int lastRead, totalRead = 0;

      while ((lastRead = readChannel.read(this.buffer)) > 0) {
        totalRead += lastRead;
      }

      LOG.debug("{} bytes read from {}", totalRead, readChannel == this.proxy ? "proxy":"client");

      if (totalRead > 0) {
        readKey.cancel();
        writeChannel.register(this.selector, SelectionKey.OP_WRITE, this);
        this.state = HandlerState.WRITING;
      }
      if (lastRead == -1) {
        readChannel.close();
      }
    }
  }

  private void closeChannels() {
    if (this.proxy.isOpen()) {
      try {
        this.proxy.close();
      } catch (IOException log) {
        LOG.warn("Failed to close proxy channel {}", this.proxy,log);
      }
    }

    if (this.client.isOpen()) {
      try {
        this.client.close();
      } catch (IOException log) {
        LOG.warn("Failed to close client channel {}", this.client,log);
      }
    }
  }
}
