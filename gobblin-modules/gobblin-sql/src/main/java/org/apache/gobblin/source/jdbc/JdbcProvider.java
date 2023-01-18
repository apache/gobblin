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

package org.apache.gobblin.source.jdbc;

import java.io.IOException;

import org.apache.gobblin.tunnel.Tunnel;

import com.zaxxer.hikari.HikariDataSource;


/**
 * Create JDBC data source
 *
 * @author nveeramr
 */
public class JdbcProvider extends HikariDataSource {
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
    this.setJdbcUrl(connectionUrl);
    // TODO: revisit following verification of successful connection pool migration:
    //   whereas `o.a.commons.dbcp.BasicDataSource` defaults min idle conns to 0, hikari defaults to 10.
    //   perhaps non-zero would have desirable runtime perf, but anything >0 currently fails unit tests (even 1!);
    //   (so experimenting with a higher number would first require adjusting tests)
    this.setMinimumIdle(0);
    this.setMaximumPoolSize(numconn);
    this.setConnectionTimeout(timeout);
  }

  @Override
  public synchronized void close() {
    super.close();
    if (this.tunnel != null) {
      this.tunnel.close();
    }
  }
}
