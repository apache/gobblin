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

package gobblin.restli;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
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
import com.linkedin.restli.server.guice.GuiceInjectResourceFactory;
import com.linkedin.restli.server.resources.BaseResource;
import com.linkedin.restli.server.resources.ResourceFactory;
import com.linkedin.restli.server.validation.RestLiValidationFilter;

import lombok.Builder;
import lombok.Getter;


/**
 * An embedded Rest.li server using Netty.
 *
 * Usage:
 * EmbeddedRestliServer server = EmbeddedRestliServer.builder().resources(List<RestliResource>).build();
 * server.startAsync()
 *
 * The server is a {@link com.google.common.util.concurrent.Service} that provides access to a collection of Rest.li
 * resources. The following are optional settings (available through builder pattern):
 * * port - defaults to randomly chosen port between {@link #MIN_PORT} and {@link #MAX_PORT}.
 * * log - defaults to class Logger.
 * * name - defaults to the name of the first resource in the resource collection.
 * * injector - an {@link Injector} to inject dependencies into the Rest.li resources.
 */
public class EmbeddedRestliServer extends AbstractIdleService {

  private static final int MAX_PORT = 65535;
  private static final int MIN_PORT = 1024;
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedRestliServer.class);

  @Getter
  private final URI serverUri;
  @Getter
  private final int port;
  @Getter
  private final Injector injector;
  private final Logger log;
  @Getter
  private final String name;
  private final Collection<Class<? extends BaseResource>> resources;
  private volatile Optional<HttpServer> httpServer;

  @Builder
  public EmbeddedRestliServer(URI serverUri, int port, Injector injector, Logger log, String name,
      Collection<Class<? extends BaseResource>> resources) {
    this.resources = resources;

    if (this.resources.isEmpty()) {
      throw new RuntimeException("No resources specified for embedded server.");
    }

    try {
      this.serverUri = serverUri == null ? new URI("http://localhost") : serverUri;
    } catch (URISyntaxException use) {
      throw new RuntimeException("Invalid URI. This is an error in code.", use);
    }
    this.port = computePort(port, this.serverUri);
    this.injector = injector == null ? Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {

      }
    }) : injector;
    this.log = log == null ? LOGGER : log;
    this.name = Strings.isNullOrEmpty(name) ? this.resources.iterator().next().getSimpleName() : name;
  }

  private final int computePort(int port, URI uri) {
    if (port > 0) {
      return port;
    } else if (uri.getPort() > 0) {
      return uri.getPort();
    } else {
      return new Random().nextInt(MAX_PORT - MIN_PORT + 1) + MIN_PORT;
    }
  }

  @Override
  protected void startUp() throws Exception {
    RestLiConfig config = new RestLiConfig();

    Set<String> resourceClassNames = Sets.newHashSet();
    for (Class<? extends BaseResource> resClass : this.resources) {
      resourceClassNames.add(resClass.getName());
    }

    config.addResourceClassNames(resourceClassNames);
    config.setServerNodeUri(this.serverUri);
    config.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());
    config.addFilter(new RestLiValidationFilter());

    ResourceFactory factory = new GuiceInjectResourceFactory(this.injector);

    TransportDispatcher dispatcher = new DelegatingTransportDispatcher(new RestLiServer(config, factory));
    String acceptedFilters = EncodingType.SNAPPY.getHttpName() + "," + EncodingType.GZIP.getHttpName();
    FilterChain filterChain = FilterChains.createRestChain(new ServerCompressionFilter(acceptedFilters));

    this.httpServer = Optional.of(new HttpNettyServerFactory(filterChain).createServer(this.port, dispatcher));
    this.log.info("Starting the {} embedded server at port {}.", this.name, this.port);
    this.httpServer.get().start();
  }

  @Override
  protected void shutDown() throws Exception {
    if (this.httpServer.isPresent()) {
      this.log.info("Stopping the {} embedded server at port {}", this.name, this.port);
      this.httpServer.get().stop();
      this.httpServer.get().waitForStop();
    }
  }

  /**
   * Get the scheme and authority at which this server is listening.
   */
  public URI getListeningURI() {
    try {
      return new URI(this.serverUri.getScheme(), this.serverUri.getUserInfo(), this.serverUri.getHost(), this.port,
          null, null, null);
    } catch (URISyntaxException use) {
      throw new RuntimeException("Invalid URI. This is an error in code.", use);
    }
  }

  /**
   * Get uri prefix that should be used to create a {@link com.linkedin.restli.client.RestClient}.
   */
  public String getURIPrefix() {
    return getListeningURI().toString() + "/";
  }
}
