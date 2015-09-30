/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.jdbc;

import org.apache.commons.dbcp.BasicDataSource;


/**
 * Jdbc data source that extends {@link org.apache.commons.dbcp.BasicDataSource} and sets necessary attributes.
 */
public class JdbcDataSource extends BasicDataSource {
  public JdbcDataSource(String driver, String connectionUrl, String user, String password, int maxIdle, int maxActive) {
    this.setDriverClassName(driver);
    this.setUsername(user);
    this.setPassword(password);
    this.setUrl(connectionUrl);
    this.setInitialSize(0);
    this.setMaxIdle(maxIdle);
    this.setMaxActive(maxActive);
  }
}
