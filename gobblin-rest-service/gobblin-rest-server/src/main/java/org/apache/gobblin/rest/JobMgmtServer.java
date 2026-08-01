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

package org.apache.gobblin.rest;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.compression.EncodingType;
import com.linkedin.r2.filter.compression.ServerCompressionFilter;
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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.JobHistoryStore;
import org.apache.gobblin.metastore.JobStoreModule;
import org.apache.gobblin.metastore.MetaStoreModule;
import org.apache.gobblin.metastore.jobstore.JobStore;
import org.apache.gobblin.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;


/**
 * A server running the Rest.li resource for job Mgmt CRUD operations.
 */
public class JobMgmtServer extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobMgmtServer.class);

  private final URI serverUri;
  private volatile Optional<HttpServer> httpServer;
  private final Config config;
  private final String host;
  private final int port;

  public JobMgmtServer(Config config) {
    host = getHost(config);
    port = getPort(config);
    serverUri = getServiceUri(host, port);
    this.config = config;
  }

  @Override
  public void startUp()
      throws Exception {
    // Server configuration
    RestLiConfig restLiConfig = new RestLiConfig();
    restLiConfig.addResourcePackageNames(JobResource.class.getPackage().getName());
    restLiConfig.setServerNodeUri(serverUri);
    restLiConfig.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());

    // Handle dependency injection
    Injector injector = Guice.createInjector(new JobStoreModule(ConfigUtils.configToProperties(config)));
    JobStore jobStore = injector.getInstance(JobStore.class);
    SimpleBeanProvider beanProvider = new SimpleBeanProvider();
    beanProvider.add("jobStore", jobStore);
    // TODO: Temp until we have seperate jobExe and Jobstore service
    Injector injectorTemp = Guice.createInjector(new MetaStoreModule(ConfigUtils.configToProperties(config)));
    JobHistoryStore jobHistoryStore = injectorTemp.getInstance(JobHistoryStore.class);
    beanProvider.add("jobHistoryStore", jobHistoryStore);

    // Use InjectMockResourceFactory to keep this Spring free
    ResourceFactory factory = new InjectMockResourceFactory(beanProvider);

    // Create and start the HTTP server
    TransportDispatcher dispatcher = new DelegatingTransportDispatcher(new RestLiServer(restLiConfig, factory));
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

  private static String getHost(Config config) {
    return ConfigUtils.getString(config, ConfigurationKeys.JOB_MGMT_SERVER_HOST_KEY, ConfigurationKeys.DEFAULT_JOB_MGMT_SERVER_HOST);
  }

  private static int getPort(Config config) {
    return Integer.parseInt(ConfigUtils.getString(config, ConfigurationKeys.JOB_SERVER_PORT_KEY, ConfigurationKeys.DEFAULT_JOB_SERVER_PORT));
  }
  private static URI getServiceUri(String host, int port) {
    return URI.create(String.format("http://%s:%d", host, port));
  }

  /*
  Main method to start the JobMgmtServer separately anywhere in the environment that has access to the metadata
  instead of clubbing it under other Gobblin processes.
   */
  public static void main(String[] argv){
    try {
      new JobMgmtServer(ConfigFactory.load()).startUp();
    }catch (Exception e){
      LOGGER.error("Error: "+ e.getMessage());
      System.exit(1);
    }
  }
}
