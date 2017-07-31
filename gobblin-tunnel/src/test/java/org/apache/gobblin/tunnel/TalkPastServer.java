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

import org.apache.commons.lang.StringUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Map;

/**
 * A TCP server that simultaneously reads and writes from a socket. This is to test the Tunnel's buffer management and
 * ensure client and server responses don't get mixed up. This server also keeps a MD5 hash of all messages received per
 * client so that tests can verify the server received what the client transmitted.
 *
 * @author kkandekar@linkedin.com
 */
class TalkPastServer extends MockServer {
  private final int nMsgs;
  private final Map<String, MessageDigest> digestMsgsRecvdAtServer;

  public TalkPastServer(int nMsgs, Map<String, MessageDigest> digestMsgsRecvdAtServer) {
    this.nMsgs = nMsgs;
    this.digestMsgsRecvdAtServer = digestMsgsRecvdAtServer;
  }

  static String generateMsgFromServer(int i) {
    return i + " " + StringUtils.repeat("Babble babble ", 10000) + "\n";
  }

  @Override
  void handleClientSocket(Socket clientSocket) throws IOException {
    LOG.info("Writing to client");
    try {
      final BufferedOutputStream serverOut = new BufferedOutputStream(clientSocket.getOutputStream());
      EasyThread clientWriterThread = new EasyThread() {
        @Override
        void runQuietly() throws Exception {
          long t = System.currentTimeMillis();
          try {
            for (int i = 0; i < nMsgs; i++) {
              serverOut.write(generateMsgFromServer(i).getBytes());
              sleepQuietly(2);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
          LOG.info("Server done writing in " + (System.currentTimeMillis() - t) + " ms");
        }
      }.startThread();
      _threads.add(clientWriterThread);

      BufferedReader serverIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      String line = serverIn.readLine();
      while (line != null && !line.equals("Goodbye")) {
        String[] tokens = line.split(":", 2);
        String client = tokens[0];
        digestMsgsRecvdAtServer.get(client).update(line.getBytes());
        digestMsgsRecvdAtServer.get(client).update("\n".getBytes());
        line = serverIn.readLine();
      }
      LOG.info("Server done reading");
      try {
        clientWriterThread.join();
      } catch (InterruptedException e) {
      }
      serverOut.write("Goodbye\n".getBytes());
      serverOut.flush();
      clientSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }
}
