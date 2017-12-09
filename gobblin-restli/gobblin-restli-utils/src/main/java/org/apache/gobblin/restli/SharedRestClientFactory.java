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

package org.apache.gobblin.restli;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import com.typesafe.config.Config;

import org.apache.gobblin.broker.ResourceCoordinate;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.ExecutorsUtils;

import io.netty.channel.nio.NioEventLoopGroup;


/**
 * A {@link SharedResourceFactory} to create {@link RestClient}s.
 *
 * To configure, specify rest server uri at key "serverUri". Note uri must start with "http" or "https".
 */
public class SharedRestClientFactory<S extends ScopeType<S>> implements SharedResourceFactory<RestClient, SharedRestClientKey, S> {

  public static final String FACTORY_NAME = "restli";
  public static final String SERVER_URI_KEY = "serverUri";

  private static final Set<String> RESTLI_SCHEMES = Sets.newHashSet("http", "https");
  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<RestClient>
    createResource(SharedResourcesBroker<S> broker, ScopedConfigView<S, SharedRestClientKey> config) throws NotConfiguredException {
    try {
      SharedRestClientKey key = config.getKey();

      if (!(key instanceof UriRestClientKey)) {
        return new ResourceCoordinate<>(this, new UriRestClientKey(key.serviceName, resolveUriPrefix(config.getConfig(), key)),
            config.getScope());
      }

      String uriPrefix = ((UriRestClientKey) key).getUri();

      HttpClientFactory http = new HttpClientFactory(FilterChains.empty(),
          new NioEventLoopGroup(0 /* use default settings */,
              ExecutorsUtils.newDaemonThreadFactory(Optional.<Logger>absent(), Optional.of("R2 Nio Event Loop-%d"))),
          true,
          Executors.newSingleThreadScheduledExecutor(
              ExecutorsUtils.newDaemonThreadFactory(Optional.<Logger>absent(), Optional.of("R2 Netty Scheduler"))),
          true);
      Client r2Client = new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));

      return new ResourceInstance<>(new RestClient(r2Client,uriPrefix));
    } catch (URISyntaxException use) {
      throw new RuntimeException("Could not create a rest client for key " + Optional.fromNullable(config.getKey().toConfigurationKey()).or("null"));
    }
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, SharedRestClientKey> config) {
    return broker.selfScope().getType().rootScope();
  }

  /**
   * Get a uri prefix from the input configuration.
   */
  public static String resolveUriPrefix(Config config, SharedRestClientKey key) throws URISyntaxException, NotConfiguredException {

    List<String> connectionPrefixes = parseConnectionPrefixes(config, key);
    Preconditions.checkArgument(connectionPrefixes.size() > 0, "No uris found for service " + key.serviceName);

    return connectionPrefixes.get(new Random().nextInt(connectionPrefixes.size()));
  }

  /**
   * Parse the list of available input prefixes from the input configuration.
   */
  public static List<String> parseConnectionPrefixes(Config config, SharedRestClientKey key) throws URISyntaxException, NotConfiguredException {
    if (key instanceof UriRestClientKey) {
      return Lists.newArrayList(((UriRestClientKey) key).getUri());
    }

    if (!config.hasPath(SERVER_URI_KEY)) {
      throw new NotConfiguredException("Missing key " + SERVER_URI_KEY);
    }

    List<String> uris = Lists.newArrayList();
    for (String uri : Splitter.on(",").omitEmptyStrings().trimResults().splitToList(config.getString(SERVER_URI_KEY))) {
      uris.add(resolveUriPrefix(new URI(uri)));
    }
    return uris;
  }

  /**
   * Convert the input URI into a correctly formatted uri prefix. In the future, may also resolve d2 uris.
   */
  public static String resolveUriPrefix(URI serverURI)
      throws URISyntaxException {
    if (RESTLI_SCHEMES.contains(serverURI.getScheme())) {
      return new URI(serverURI.getScheme(), serverURI.getAuthority(), null, null, null).toString() + "/";
    }

    throw new RuntimeException("Unrecognized scheme for URI " + serverURI);
  }
}
