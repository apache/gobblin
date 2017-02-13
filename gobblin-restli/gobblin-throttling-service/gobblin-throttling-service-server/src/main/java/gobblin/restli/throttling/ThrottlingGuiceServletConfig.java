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

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.linkedin.r2.filter.CompressionConfig;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.compression.EncodingType;
import com.linkedin.r2.filter.compression.ServerCompressionFilter;
import com.linkedin.r2.filter.logging.SimpleLoggingFilter;
import com.linkedin.restli.server.RestLiConfig;
import com.linkedin.restli.server.guice.GuiceRestliServlet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gobblin.broker.BrokerConstants;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.SimpleScopeType;
import gobblin.broker.iface.SharedResourcesBroker;
import java.util.Enumeration;
import java.util.Map;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import lombok.extern.slf4j.Slf4j;


/**
 * {@link GuiceServletContextListener} for creating an injector in a gobblin-throttling-server servlet.
 */
@Slf4j
public class ThrottlingGuiceServletConfig extends GuiceServletContextListener {

  private ServletContext _context;
  private Config _brokerConfig;

  @Override
  public void contextInitialized(ServletContextEvent servletContextEvent) {
    _context = servletContextEvent.getServletContext();

    Enumeration<String> parameters = _context.getInitParameterNames();
    Map<String, String> brokerConfigMap = Maps.newHashMap();
    while (parameters.hasMoreElements()) {
      String key = parameters.nextElement();
      if (key.startsWith(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX)) {
        brokerConfigMap.put(key, _context.getInitParameter(key));
      }
    }
    this._brokerConfig = ConfigFactory.parseMap(brokerConfigMap);

    super.contextInitialized(servletContextEvent);
  }

  @Override
  protected Injector getInjector() {
    return Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        RestLiConfig restLiConfig = new RestLiConfig();
        restLiConfig.setResourcePackageNames("gobblin.restli.throttling");
        bind(RestLiConfig.class).toInstance(restLiConfig);

        SharedResourcesBroker<SimpleScopeType> broker =
            SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ThrottlingGuiceServletConfig.this._brokerConfig,
                SimpleScopeType.GLOBAL.defaultScopeInstance());
        bind(SharedResourcesBroker.class).toInstance(broker);

        FilterChain filterChain = FilterChains.create(
            new ServerCompressionFilter(new EncodingType[] { EncodingType.SNAPPY }),
            new SimpleLoggingFilter());
        bind(FilterChain.class).toInstance(filterChain);
      }
    },
        new ServletModule()
        {
          @Override
          protected void configureServlets()
          {
            serve("/*").with(GuiceRestliServlet.class);
          }
        });
  }
}
