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

package gobblin.source.jdbc;

import gobblin.tunnel.Tunnel;
import org.apache.commons.dbcp.BasicDataSource;

import java.io.IOException;
import java.sql.SQLException;


/**
 * Create JDBC data source
 *
 * @author nveeramr
 */
public class JdbcProvider extends BasicDataSource {
  private Tunnel tunnel;

  // If extract type is not provided then consider it as a default type
  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, "DEFAULT", null, -1);
  }

  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, type, null, -1);
  }

  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type, String proxyHost, int proxyPort) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, type, proxyHost, proxyPort);
  }

  public void connect(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type, String proxyHost, int proxyPort) {

    if (proxyHost != null && proxyPort > 0) {
      String remoteHost = "";
      int remotePort = 0;
      // TODO make connection Url parsing much more robust -- some connections URLs can have colons and slashes in the
      // weirdest places
      int hostStart = connectionUrl.indexOf("://") + 3;
      int portStart = connectionUrl.indexOf(":", hostStart);
      remoteHost = connectionUrl.substring(hostStart, portStart);
      remotePort = Integer.decode(connectionUrl.substring(portStart + 1, connectionUrl.indexOf("/", portStart)));

      try {
        this.tunnel = Tunnel.build(remoteHost, remotePort, proxyHost, proxyPort);
        int tunnelPort = this.tunnel.getPort();
        //mangle connectionUrl, replace hostname with localhost -- hopefully the hostname is not needed!!!
        String newConnectionUrl =
            connectionUrl.replaceFirst(remoteHost, "127.0.0.1").replaceFirst(":" + remotePort, ":" + tunnelPort);
        connectionUrl = newConnectionUrl;
      } catch (IOException ioe) {
        throw new IllegalStateException("Failed to open tunnel to remote host " + remoteHost + ":" + remotePort
            + " via proxy " + proxyHost + ":" + proxyPort, ioe);
      }
    }

    this.setDriverClassName(driver);
    this.setUsername(user);
    this.setPassword(password);
    this.setUrl(connectionUrl);
    this.setInitialSize(0);
    this.setMaxIdle(numconn);
    this.setMaxWait(timeout);
  }

  @Override
  public synchronized void close() throws SQLException {
    super.close();
    if (this.tunnel != null) {
      this.tunnel.close();
    }
  }
}
