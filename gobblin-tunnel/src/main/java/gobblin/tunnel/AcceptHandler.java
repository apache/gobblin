package gobblin.tunnel;

import java.io.IOException;
import java.nio.channels.SelectionKey;
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

  AcceptHandler(ServerSocketChannel server, Selector selector, Config config)
      throws IOException {
    this.config = config;
    this.server = server;
    this.selector = selector;

    this.server.configureBlocking(false);
    this.server.register(selector, SelectionKey.OP_ACCEPT, this);
  }

  @Override
  public HandlerState call()
      throws Exception {
    SocketChannel client = server.accept();

    LOG.info("Accepted connection from {}", client.getRemoteAddress());
    try {
      new ProxySetupHandler(client, selector, config);
    } catch (IOException ioe) {
      client.close();
    }
    return HandlerState.ACCEPTING;
  }
}
