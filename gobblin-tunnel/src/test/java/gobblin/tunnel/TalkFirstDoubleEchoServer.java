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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

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
    out.println("Hello");
    out.flush();

    String line = in.readLine();
    while (line != null && isServerRunning()) {
      out.println(line + " " + line);
      out.flush();
      line = in.readLine();
    }
    clientSocket.close();
  }
}
