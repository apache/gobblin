/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.jdbc;

import org.apache.commons.dbcp.BasicDataSource;


/**
 * Create JDBC data source
 *
 * @author nveeramr
 */
public class JdbcProvider extends BasicDataSource {
  // If extract type is not provided then consider it as a default type
  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, "DEFAULT");
  }

  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, type);
  }

  public void connect(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type) {
    this.setDriverClassName(driver);
    this.setUsername(user);
    this.setPassword(password);
    this.setUrl(connectionUrl);
    this.setInitialSize(0);
    this.setMaxIdle(numconn);
    this.setMaxActive(timeout);
  }
}