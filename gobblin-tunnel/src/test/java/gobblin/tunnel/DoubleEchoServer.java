package gobblin.tunnel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * A TCP server that reads a line from a socket, trims it, and echoes it back twice on the same line.
 *
 * @author kkandekar@linkedin.com
 */
class DoubleEchoServer extends MockServer {
  private final long delay;

  public DoubleEchoServer(long delay) {
    this.delay = delay;
  }

  @Override
  void handleClientSocket(Socket clientSocket) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
    String line = in.readLine();
    while (line != null && isServerRunning()) {
      if (delay > 0) {
        sleepQuietly(delay);
      }
      out.println(line + " " + line);
      out.flush();
      line = in.readLine();
    }
    clientSocket.close();
  }
}
