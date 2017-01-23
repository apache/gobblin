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

package gobblin.service;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
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
import com.linkedin.restli.server.resources.ResourceFactory;
import com.typesafe.config.Config;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.runtime.job_catalog.FSJobCatalog;
import gobblin.util.ConfigUtils;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Server that handles Rest.li requests over HTTP to configure Gobblin Service jobs
 */
public class JobConfigServer {
  private static final Logger LOG = LoggerFactory.getLogger(JobConfigServer.class);

  public static final String SERVICE_PORT_KEY = "gobblin.service.port";
  public static final int DEFAULT_SERVICE_PORT = 8080;
  public static final String SERVICE_HOST_KEY = "gobblin.service.host";
  public static final String DEFAULT_SERVICE_HOST = "localhost";

  private final URI serverUri;
  private final int port;
  private volatile Optional<HttpServer> httpServer;
  private volatile boolean shuttingDown = false;
  private final ResourceFactory factory;

  /**
   * Create a {@link JobConfigServer} configured with properties
   * @param config the server host and port properties
   */
  public JobConfigServer(final Config config, final MutableJobCatalog jobCatalog) {
    port = ConfigUtils.getInt(config, SERVICE_PORT_KEY, DEFAULT_SERVICE_PORT);
    String host = ConfigUtils.getString(config, SERVICE_HOST_KEY, DEFAULT_SERVICE_HOST);

    serverUri = getServiceUri(host, port);

    final ServiceManager serviceManager;

    try {
      serviceManager = new ServiceManager(Collections.singletonList((Service)jobCatalog));
      serviceManager.startAsync();
      serviceManager.awaitHealthy(10, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new RuntimeException("Timedout " + e, e);
    }

    factory = new JobConfigsResourceFactory(jobCatalog);
  }

  /**
   * Start the http server to handle rest.li requests
   * @throws Exception
   */
  protected void startUp()
      throws Exception {
    // Server configuration
    RestLiConfig config = new RestLiConfig();
    config.addResourcePackageNames(JobConfigsResource.class.getPackage().getName());
    config.setServerNodeUri(serverUri);
    config.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());

    // Create and start the HTTP server
    TransportDispatcher dispatcher = new DelegatingTransportDispatcher(new RestLiServer(config, factory));
    FilterChain filterChain = FilterChains.create(new ServerCompressionFilter(new EncodingType[] {
        EncodingType.SNAPPY,
        EncodingType.GZIP
    }));
    httpServer = Optional.of(new HttpNettyServerFactory(filterChain).createServer(port, dispatcher));
    LOG.info("Starting the job configuration server");
    httpServer.get().start();
  }

  /**
   * Shutdown the http server
   */
  protected void shutDown() {
    if (httpServer.isPresent()) {
      LOG.info("Stopping the job execution information server");
      try {
        httpServer.get().stop();
      } catch (IOException e) {
        LOG.error("Error Stopping the server: " + e);
      }
    }
    shuttingDown = true;
  }

  private static URI getServiceUri(String host, int port) {
    return URI.create(String.format("http://%s:%d", host, port));
  }

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    try (InputStream propStream = new FileInputStream(args[0])) {
      props.load(propStream);
    }

    Config config = ConfigUtils.propertiesToConfig(props);
    final MutableJobCatalog jobCatalog = new FSJobCatalog(config);
    final JobConfigServer server = new JobConfigServer(config, jobCatalog);
    final ReentrantLock lock = new ReentrantLock();
    final Condition shutdownCondition = lock.newCondition();
    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        lock.lock();
        LOG.info("Starting the shutdown process..");
        server.shutDown();
        shutdownCondition.signalAll();
      }
    });

    lock.lock();
    server.startUp();
    while (!server.shuttingDown) {
      shutdownCondition.await();
    }
    LOG.info("Main thread is exiting...");
  }
}
