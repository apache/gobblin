/*
 * Copyright (C) 2014-2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.restli.throttling;

import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.compression.EncodingType;
import com.linkedin.r2.filter.compression.ServerCompressionFilter;
import com.linkedin.r2.filter.logging.SimpleLoggingFilter;
import com.linkedin.r2.filter.message.rest.RestFilter;
import com.linkedin.r2.filter.message.stream.StreamFilter;
import com.linkedin.restli.server.RestLiConfig;
import com.linkedin.restli.server.guice.GuiceRestliServlet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.MetricContext;
import gobblin.metrics.broker.MetricContextFactory;
import gobblin.metrics.broker.MetricContextKey;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * {@link GuiceServletContextListener} for creating an injector in a gobblin-throttling-server servlet.
 */
@Slf4j
@Getter
public class ThrottlingGuiceServletConfig extends GuiceServletContextListener implements Closeable {

  public static final String THROTTLING_SERVER_PREFIX = "throttlingServer.";
  public static final String LISTENING_PORT = THROTTLING_SERVER_PREFIX + "listeningPort";
  public static final String HOSTNAME = THROTTLING_SERVER_PREFIX + "hostname";

  public static final String ZK_STRING_KEY = THROTTLING_SERVER_PREFIX + "ha.zkString";
  public static final String HA_CLUSTER_NAME = THROTTLING_SERVER_PREFIX + "ha.clusterName";

  private Optional<LeaderFinder<URIMetadata>> _leaderFinder;
  private Config _config;
  private Injector _injector;

  @Override
  public void contextInitialized(ServletContextEvent servletContextEvent) {
    ServletContext context = servletContextEvent.getServletContext();

    Enumeration<String> parameters = context.getInitParameterNames();
    Map<String, String> configMap = Maps.newHashMap();
    while (parameters.hasMoreElements()) {
      String key = parameters.nextElement();
      configMap.put(key, context.getInitParameter(key));
    }
    initialize(ConfigFactory.parseMap(configMap));

    super.contextInitialized(servletContextEvent);
  }

  public void initialize(Config config) {
    try {
      this._config = config;
      this._leaderFinder = getLeaderFinder(this._config);
      if (this._leaderFinder.isPresent()) {
        this._leaderFinder.get().startAsync();
        this._leaderFinder.get().awaitRunning(100, TimeUnit.SECONDS);
      }
      this._injector = createInjector(this._config, this._leaderFinder);
    } catch (URISyntaxException | IOException | TimeoutException exc) {
      log.error(String.format("Error in %s initialization.", ThrottlingGuiceServletConfig.class.getSimpleName()), exc);
      throw new RuntimeException(exc);
    }
  }

  @Override
  public Injector getInjector() {
    return this._injector;
  }

  private Injector createInjector(final Config config, final Optional<LeaderFinder<URIMetadata>> leaderFinder) {
    final SharedResourcesBroker<ThrottlingServerScopes> topLevelBroker =
        SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config, ThrottlingServerScopes.GLOBAL.defaultScopeInstance());

    return Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        try {

          RestLiConfig restLiConfig = new RestLiConfig();
          restLiConfig.setResourcePackageNames("gobblin.restli.throttling");
          bind(RestLiConfig.class).toInstance(restLiConfig);

          bind(SharedResourcesBroker.class).annotatedWith(Names.named(LimiterServerResource.BROKER_INJECT_NAME)).toInstance(topLevelBroker);

          MetricContext metricContext =
              topLevelBroker.getSharedResource(new MetricContextFactory<ThrottlingServerScopes>(), new MetricContextKey());
          Timer timer = metricContext.timer(LimiterServerResource.REQUEST_TIMER_NAME);

          bind(MetricContext.class).annotatedWith(Names.named(LimiterServerResource.METRIC_CONTEXT_INJECT_NAME)).toInstance(metricContext);
          bind(Timer.class).annotatedWith(Names.named(LimiterServerResource.REQUEST_TIMER_INJECT_NAME)).toInstance(timer);

          bind(new TypeLiteral<Optional<LeaderFinder<URIMetadata>>>() {
          }).annotatedWith(Names.named(LimiterServerResource.LEADER_FINDER_INJECT_NAME)).toInstance(leaderFinder);

          List<RestFilter> restFilters = new ArrayList<>();
          restFilters.add(new ServerCompressionFilter(EncodingType.SNAPPY.getHttpName()));
          List<StreamFilter> streamFilters = new ArrayList<>();
          streamFilters.add(new SimpleLoggingFilter());
          FilterChain filterChain = FilterChains.create(restFilters, streamFilters);
          bind(FilterChain.class).toInstance(filterChain);
        } catch (NotConfiguredException nce) {
          throw new RuntimeException(nce);
        }
      }
    }, new ServletModule() {
      @Override
      protected void configureServlets() {
        serve("/*").with(GuiceRestliServlet.class);
      }
    });
  }

  private static Optional<LeaderFinder<URIMetadata>> getLeaderFinder(Config config) throws URISyntaxException,
                                                                                           IOException {
    if (config.hasPath(ZK_STRING_KEY)) {
      Preconditions.checkArgument(config.hasPath(LISTENING_PORT), "Missing required config " + LISTENING_PORT);
      Preconditions.checkArgument(config.hasPath(HA_CLUSTER_NAME), "Missing required config " + HA_CLUSTER_NAME);

      int port = config.getInt(LISTENING_PORT);
      String hostname = config.hasPath(HOSTNAME) ? config.getString(HOSTNAME) : InetAddress.getLocalHost().getCanonicalHostName();

      String clusterName = config.getString(HA_CLUSTER_NAME);
      String zkString = config.getString(ZK_STRING_KEY);

      return Optional.<LeaderFinder<URIMetadata>>of(new ZookeeperLeaderElection<>(zkString, clusterName,
          new URIMetadata(new URI("http", null, hostname, port, null, null, null))));
    }
    return Optional.absent();
  }

  @Override
  public void contextDestroyed(ServletContextEvent servletContextEvent) {
    close();
    super.contextDestroyed(servletContextEvent);
  }

  @Override
  public void close() {
    try {
      if (this._leaderFinder.isPresent()) {
        this._leaderFinder.get().stopAsync();
        this._leaderFinder.get().awaitTerminated(2, TimeUnit.SECONDS);
      }
    } catch (TimeoutException te) {
      // Do nothing
    }
  }

  /**
   * Get an instance of {@link LimiterServerResource}.
   */
  public LimiterServerResource getLimiterResource() {
    return this._injector.getInstance(LimiterServerResource.class);
  }
}
