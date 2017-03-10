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

package gobblin.metrics.broker;

import com.typesafe.config.ConfigValue;
import gobblin.broker.ResourceInstance;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NoSuchScopeException;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceFactoryResponse;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.MetricContext;
import gobblin.metrics.RootMetricContext;
import gobblin.metrics.Tag;
import gobblin.util.ConfigUtils;
import java.util.Collection;
import java.util.Map;


/**
 * A {@link SharedResourceFactory} to create {@link MetricContext}.
 *
 * The created {@link MetricContext} tree will mimic a sub-tree of the scopes DAG. If each scope has a unique parent,
 * the metric contexts will have the corresponding parents. If a scope has multiple parents (which is not supported by
 * {@link MetricContext}), the factory will chose the first parent of the scope.
 *
 * Tags can be injected using the configuration {@link Tag}.
 */
public class MetricContextFactory<S extends ScopeType<S>> implements SharedResourceFactory<MetricContext, MetricContextKey, S> {

  public static final String NAME = "metricContext";

  public static final String TAG_KEY = "tag";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SharedResourceFactoryResponse<MetricContext> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, MetricContextKey> config) throws NotConfiguredException {

    try {
      MetricContext parentMetricContext = RootMetricContext.get();

      Collection<S> parents = config.getScope().parentScopes();
      if (parents != null && !parents.isEmpty()) {
        S parentScope = parents.iterator().next();
        parentMetricContext = broker.getSharedResourceAtScope(this, config.getKey(), parentScope);
      }

      MetricContext.Builder builder = parentMetricContext.childBuilder(broker.selfScope().toString());

      builder.addTag(new Tag<>(config.getScope().name(), broker.getScope(config.getScope()).getScopeId()));
      for (Map.Entry<String, ConfigValue> entry : ConfigUtils.getConfigOrEmpty(config.getConfig(), TAG_KEY).entrySet()) {
        builder.addTag(new Tag<>(entry.getKey(), entry.getValue().unwrapped()));
      }

      return new ResourceInstance<>(builder.build());
    } catch (NoSuchScopeException nsse) {
      throw new RuntimeException("Could not create MetricContext.", nsse);
    }
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, MetricContextKey> config) {
    return broker.selfScope().getType();
  }
}
