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
package org.apache.gobblin.admin;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Properties;


/**
 * Serves the admin UI interface using embedded Jetty.
 */
public class AdminWebServer extends AbstractIdleService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminWebServer.class);

  private final URI restServerUri;
  private final URI serverUri;
  private final String hideJobsWithoutTasksByDefault;
  private final long refreshInterval;
  protected Server server;

  public AdminWebServer(Properties properties, URI restServerUri) {
    Preconditions.checkNotNull(properties);
    Preconditions.checkNotNull(restServerUri);

    this.restServerUri = restServerUri;
    int port = getPort(properties);
    this.serverUri = URI.create(String.format("http://%s:%d", getHost(properties), port));
    this.hideJobsWithoutTasksByDefault = properties.getProperty(
            ConfigurationKeys.ADMIN_SERVER_HIDE_JOBS_WITHOUT_TASKS_BY_DEFAULT_KEY,
            ConfigurationKeys.DEFAULT_ADMIN_SERVER_HIDE_JOBS_WITHOUT_TASKS_BY_DEFAULT);
    this.refreshInterval = getRefreshInterval(properties);
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the admin web server");

    this.server = new Server(new InetSocketAddress(this.serverUri.getHost(), this.serverUri.getPort()));

    HandlerCollection handlerCollection = new HandlerCollection();

    handlerCollection.addHandler(buildSettingsHandler());
    handlerCollection.addHandler(buildStaticResourceHandler());

    this.server.setHandler(handlerCollection);
    this.server.start();
  }

  private Handler buildSettingsHandler() {
    final String responseTemplate = "var Gobblin = window.Gobblin || {};" + "Gobblin.settings = {restServerUrl:\"%s\", hideJobsWithoutTasksByDefault:%s, refreshInterval:%s}";

    return new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        if (request.getRequestURI().equals("/js/settings.js")) {
          response.setContentType("application/javascript");
          response.setStatus(HttpServletResponse.SC_OK);
          response.getWriter().println(String.format(responseTemplate, AdminWebServer.this.restServerUri.toString(),
              AdminWebServer.this.hideJobsWithoutTasksByDefault, AdminWebServer.this.refreshInterval));
          baseRequest.setHandled(true);
        }
      }
    };
  }

  private ResourceHandler buildStaticResourceHandler() {
    ResourceHandler staticResourceHandler = new ResourceHandler();
    staticResourceHandler.setDirectoriesListed(true);
    staticResourceHandler.setWelcomeFiles(new String[] { "index.html" });

    String staticDir = getClass().getClassLoader().getResource("static").toExternalForm();

    staticResourceHandler.setResourceBase(staticDir);
    return staticResourceHandler;
  }

  @Override
  protected void shutDown() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
  }

  private static int getPort(Properties properties) {
    return Integer.parseInt(
        properties.getProperty(ConfigurationKeys.ADMIN_SERVER_PORT_KEY, ConfigurationKeys.DEFAULT_ADMIN_SERVER_PORT));
  }

  private static String getHost(Properties properties) {
    return properties.getProperty(ConfigurationKeys.ADMIN_SERVER_HOST_KEY, ConfigurationKeys.DEFAULT_ADMIN_SERVER_HOST);
  }

  private static long getRefreshInterval(Properties properties) {
    return Long.parseLong(
        properties.getProperty(ConfigurationKeys.ADMIN_SERVER_REFRESH_INTERVAL_KEY,
                "" + ConfigurationKeys.DEFAULT_ADMIN_SERVER_REFRESH_INTERVAL));
  }
}
