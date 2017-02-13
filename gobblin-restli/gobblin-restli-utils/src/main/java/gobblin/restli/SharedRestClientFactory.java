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
import java.util.Collections;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;

import gobblin.broker.ResourceInstance;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourcesBroker;


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
  public ResourceInstance<RestClient> createResource(SharedResourcesBroker broker, ScopedConfigView<?, SharedRestClientKey> config)
      throws NotConfiguredException {
    try {
      String uriPrefix = resolveUriPrefix(config);

      HttpClientFactory http = new HttpClientFactory();
      Client r2Client = new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));

      return new ResourceInstance<>(new RestClient(r2Client,uriPrefix));
    } catch (URISyntaxException use) {
      throw new RuntimeException("Could not create a rest client for key " + config.getKey().toConfigurationKey());
    }
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, SharedRestClientKey> config) {
    return broker.selfScope().getType().rootScope();
  }

  private String resolveUriPrefix(ScopedConfigView<?, SharedRestClientKey> config) throws URISyntaxException {
    Preconditions.checkArgument(config.getConfig().hasPath(SERVER_URI_KEY));

    URI serverURI = new URI(config.getConfig().getString(SERVER_URI_KEY));

    if (RESTLI_SCHEMES.contains(serverURI.getScheme())) {
      return new URI(serverURI.getScheme(), serverURI.getAuthority(), null, null, null).toString() + "/";
    }

    throw new RuntimeException("Could not parse URI prefix for key " + config.getKey().toConfigurationKey());
  }
}
