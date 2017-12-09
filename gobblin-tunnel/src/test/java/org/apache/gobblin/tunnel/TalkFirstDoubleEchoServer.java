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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

/**
 * A double-echo TCP server that writes to a socket as soon as it accepts one. This is to simulate the behavior of
 * certain protocols like MySql.
 *
 * @author kkandekar@linkedin.com
 */
class TalkFirstDoubleEchoServer extends MockServer {
  @Override
  void handleClientSocket(Socket clientSocket) throws IOException {
    LOG.info("Writing to client");

    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    PrintWriter out = new PrintWriter(clientSocket.getOutputStream());

    try {
      out.println("Hello");
      out.flush();

      String line = in.readLine();
      while (line != null && isServerRunning()) {
        out.println(line + " " + line);
        out.flush();
        line = in.readLine();
      }
    }
    catch (SocketException se) {
      // don't bring down server when client disconnected abruptly
      if (!se.getMessage().contains("Connection reset")) {
        throw se;
      }
    }
    finally {
      clientSocket.close();
    }
  }
}
