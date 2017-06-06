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

package gobblin.rest;

import java.net.URI;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;

import com.linkedin.r2.filter.compression.EncodingType;
import com.linkedin.r2.filter.compression.ServerCompressionFilter;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.http.server.HttpNettyServerFactory;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.restli.docgen.DefaultDocumentationRequestHandler;
import com.linkedin.restli.server.DelegatingTransportDispatcher;
import com.linkedin.restli.server.RestLiConfig;
import com.linkedin.restli.server.RestLiServer;
import com.linkedin.restli.server.mock.InjectMockResourceFactory;
import com.linkedin.restli.server.mock.SimpleBeanProvider;
import com.linkedin.restli.server.resources.ResourceFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.JobHistoryStore;
import gobblin.metastore.MetaStoreModule;


/**
 * A server running the Rest.li resource for job execution queries.
 *
 * @author Yinan Li
 */
public class JobExecutionInfoServer extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionInfoServer.class);

  private final URI serverUri;
  private final URI serverAdvertisedUri;
  private final int port;
  private final Properties properties;
  private volatile Optional<HttpServer> httpServer;

  public JobExecutionInfoServer(Properties properties) {
    this.properties = properties;

    port = getPort(properties);
    serverUri = getServiceUri(getHost(properties), port);
    serverAdvertisedUri = getAdvertisedUri(properties);
  }

  @Override
  protected void startUp()
      throws Exception {
    // Server configuration
    RestLiConfig config = new RestLiConfig();
    config.addResourcePackageNames(JobExecutionInfoResource.class.getPackage().getName());
    config.setServerNodeUri(serverUri);
    config.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());

    // Handle dependency injection
    Injector injector = Guice.createInjector(new MetaStoreModule(properties));
    JobHistoryStore jobHistoryStore = injector.getInstance(JobHistoryStore.class);
    SimpleBeanProvider beanProvider = new SimpleBeanProvider();
    beanProvider.add("jobHistoryStore", jobHistoryStore);
    // Use InjectMockResourceFactory to keep this Spring free
    ResourceFactory factory = new InjectMockResourceFactory(beanProvider);

    // Create and start the HTTP server
    TransportDispatcher dispatcher = new DelegatingTransportDispatcher(new RestLiServer(config, factory));
    String acceptedFilters = EncodingType.SNAPPY.getHttpName() + "," + EncodingType.GZIP.getHttpName();
    FilterChain filterChain = FilterChains.createRestChain(new ServerCompressionFilter(acceptedFilters));
    this.httpServer = Optional.of(new HttpNettyServerFactory(filterChain).createServer(port, dispatcher));
    LOGGER.info("Starting the job execution information server");
    this.httpServer.get().start();
  }

  @Override
  protected void shutDown()
      throws Exception {
    if (this.httpServer.isPresent()) {
      LOGGER.info("Stopping the job execution information server");
      this.httpServer.get().stop();
    }
  }

  public URI getAdvertisedServerUri() {
    return serverAdvertisedUri;
  }

  private static URI getServiceUri(String host, int port) {
    return URI.create(String.format("http://%s:%d", host, port));
  }

  private static int getPort(Properties properties) {
    return Integer.parseInt(properties.getProperty(
            ConfigurationKeys.REST_SERVER_PORT_KEY,
            ConfigurationKeys.DEFAULT_REST_SERVER_PORT));
  }

  private static String getHost(Properties properties) {
    return properties.getProperty(
            ConfigurationKeys.REST_SERVER_HOST_KEY,
            ConfigurationKeys.DEFAULT_REST_SERVER_HOST);
  }

  private static URI getAdvertisedUri(Properties properties) {
    return URI.create(properties.getProperty(
            ConfigurationKeys.REST_SERVER_ADVERTISED_URI_KEY,
            getServiceUri(getHost(properties), getPort(properties)).toString()));
  }
}
