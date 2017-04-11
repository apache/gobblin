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

package gobblin.runtime.instance;

import java.util.List;

import org.slf4j.Logger;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.configuration.State;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.GobblinInstanceEnvironment;

import javax.annotation.Nonnull;
import lombok.Data;


/**
 * A barebones implementation of {@link GobblinInstanceEnvironment} used to inject system configurations to
 * {@link StandardGobblinInstanceDriver}.
 */
@Data
public class SimpleGobblinInstanceEnvironment implements GobblinInstanceEnvironment {

  private final String instanceName;
  private final Logger log;
  private final Configurable sysConfig;

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return false;
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    return null;
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SharedResourcesBroker<GobblinScopeTypes> getInstanceBroker() {
    throw new UnsupportedOperationException();
  }
}
