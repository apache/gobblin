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

package gobblin.metastore.util;

import org.apache.http.client.utils.URIBuilder;

import java.net.MalformedURLException;
import java.net.URISyntaxException;


public class MySqlJdbcUrl {
  private static final String PREFIX = "jdbc:";
  private final URIBuilder builder;

  private MySqlJdbcUrl() {
    builder = new URIBuilder();
    builder.setScheme("mysql");
  }

  private MySqlJdbcUrl(String url) throws MalformedURLException, URISyntaxException {
    if (!url.startsWith(PREFIX)) {
    throw new MalformedURLException();
    }
    builder = new URIBuilder(url.substring(PREFIX.length()));
  }

  public static MySqlJdbcUrl create() {
    return new MySqlJdbcUrl();
  }

  public static MySqlJdbcUrl parse(String url) throws MalformedURLException, URISyntaxException {
    return new MySqlJdbcUrl(url);
  }

  public MySqlJdbcUrl setHost(String host) {
    builder.setHost(host);
    return this;
  }

  public MySqlJdbcUrl setPort(int port) {
    builder.setPort(port);
    return this;
  }

  public MySqlJdbcUrl setPath(String path) {
    builder.setPath("/" + path);
    return this;
  }

  public MySqlJdbcUrl setUser(String user) {
    return setParameter("user", user);
  }

  public MySqlJdbcUrl setPassword(String password) {
    return setParameter("password", password);
  }

  public MySqlJdbcUrl setParameter(String param, String value) {
    builder.setParameter(param, value);
    return this;
  }

  @Override
  public String toString() {
    try {
      return PREFIX + builder.build().toString();
    } catch (URISyntaxException e) {
      return "";
    }
  }
}
