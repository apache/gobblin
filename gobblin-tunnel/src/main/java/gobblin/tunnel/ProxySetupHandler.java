package gobblin.tunnel;

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

import org.slf4j.LoggerFactory;

/**
 * Handler for setting up the connection from the Tunnel to the intermediate proxy via HTTP CONNECT.
 */
class ProxySetupHandler implements Callable<HandlerState> {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Tunnel.class);

  private static final ByteBuffer OK_REPLY = ByteBuffer.wrap("HTTP/1.1 200".getBytes());
  private static final Set<ByteBuffer> OK_REPLIES = new HashSet<ByteBuffer>(
      Arrays.asList(OK_REPLY, ByteBuffer.wrap("HTTP/1.0 200".getBytes())));


  private final SocketChannel client;
  private final Selector selector;
  private final SocketChannel proxy;
  private HandlerState state = HandlerState.CONNECTING;

  private ByteBuffer buffer;
  private final long connectStartTime;
  private int totalBytesRead = 0;
  private final Config config;

  ProxySetupHandler(SocketChannel client, Selector selector, Config config)
      throws IOException {
    this.config = config;
    this.client = client;
    this.selector = selector;
    buffer = ByteBuffer.wrap(
        String.format("CONNECT %s:%d HTTP/1.1\r\nUser-Agent: GobblinTunnel\r\nservice-name: gobblin\r\n" +
                "Connection: keep-alive\r\nHost: %s:%d\r\n\r\n",
            config.getRemoteHost(), config.getRemotePort(),
            config.getRemoteHost(), config.getRemotePort())
            .getBytes());

    //Blocking call
    proxy = SocketChannel.open();
    proxy.configureBlocking(false);
    connectStartTime = System.currentTimeMillis();
    boolean connected = proxy.connect(new InetSocketAddress(this.config.getProxyHost(), this.config.getProxyPort()));

    if (!connected) {
      this.client.configureBlocking(false);
      this.client.register(this.selector, SelectionKey.OP_READ, this);
      proxy.register(this.selector, SelectionKey.OP_CONNECT, this);
    } else {
      state = HandlerState.WRITING;
      proxy.register(this.selector, SelectionKey.OP_WRITE, this);
    }
  }

  @Override
  public HandlerState call()
      throws Exception {
    try {
      switch (state) {
        case CONNECTING:
          connect();
          break;
        case WRITING:
          write();
          break;
        case READING:
          read();
          break;
      }
    } catch (IOException ioe) {
      LOG.warn("Failed to establish a proxy connection for {}", client.getRemoteAddress(), ioe);
      closeChannels();
    }
    return state;
  }

  private void connect() throws IOException {
    if (proxy.isOpen()) {
      if (proxy.finishConnect()) {
        proxy.register(selector, SelectionKey.OP_WRITE, this);
        SelectionKey clientKey = client.keyFor(selector);
        if (clientKey != null) {
          clientKey.cancel();
        }
        state = HandlerState.WRITING;
      } else if (connectStartTime + Config.PROXY_CONNECT_TIMEOUT_MS < System.currentTimeMillis()) {
        LOG.warn("Proxy connect timed out for client {}", client);
        closeChannels();
      }
    }
  }

  private void write() throws IOException {
    while (proxy.write(buffer) > 0) {
    }

    if (buffer.remaining() == 0) {
      proxy.register(selector, SelectionKey.OP_READ, this);
      state = HandlerState.READING;
      buffer = ByteBuffer.allocate(1024);
    }
  }

  private void read()
      throws IOException {
    int lastBytes = 0;

    while ((lastBytes = proxy.read(buffer)) > 0) {
      // totalBytesRead has to be stateful because read() might return at arbitrary points
      totalBytesRead += lastBytes;
    }

    if (totalBytesRead >= OK_REPLY.limit()) {
      byte[] temp = buffer.array();
      buffer.flip();
      if (OK_REPLIES.contains(ByteBuffer.wrap(temp, 0, OK_REPLY.limit()))) {
        // drain the rest of the HTTP response. 2 consecutive CRLFs signify the end of an HTTP
        // message (some proxies return newlines instead of CRLFs)
        for (int i = OK_REPLY.limit(); i <= (buffer.limit() - 4); i++) {
          if (((temp[i] == '\n') && (temp[i + 1] == '\n'))
              || ((temp[i + 1] == '\n') && (temp[i + 2] == '\n'))
              || ((temp[i + 2] == '\n') && (temp[i + 3] == '\n'))
              || ((temp[i] == '\r') && (temp[i + 1] == '\n') && (temp[i + 2] == '\r') && (temp[i + 3] == '\n'))) {
            state = null;
            buffer.position(i + 4);
            new ReadWriteHandler(proxy, buffer, client, selector);
            return;
          }
        }
      } else {
        LOG.error("Got non-200 response from proxy: [" + new String(temp, 0, OK_REPLY.limit())
            + "], closing connection.");
        closeChannels();
      }
    }
  }

  private void closeChannels() {
    if (proxy.isOpen()) {
      try {
        proxy.close();
      } catch (IOException log) {
        LOG.warn("Failed to close proxy channel {}", proxy, log);
      }
    }

    if (client.isOpen()) {
      try {
        client.close();
      } catch (IOException log) {
        LOG.warn("Failed to close client channel {}", client, log);
      }
    }
  }
}
