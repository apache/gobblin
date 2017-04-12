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

package gobblin.runtime.plugins.metrics;

import gobblin.annotation.Alias;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.GobblinInstancePlugin;
import gobblin.runtime.api.GobblinInstancePluginFactory;
import gobblin.runtime.instance.plugin.BaseIdlePluginImpl;


/**
 * A {@link GobblinInstancePlugin} for enabling metrics.
 */
public class GobblinMetricsPlugin extends BaseIdlePluginImpl {

  @Alias("metrics")
  public static class Factory implements GobblinInstancePluginFactory {
    @Override
    public GobblinInstancePlugin createPlugin(GobblinInstanceDriver instance) {
      return new GobblinMetricsPlugin(instance);
    }
  }

  private final GobblinMetrics metrics;

  public GobblinMetricsPlugin(GobblinInstanceDriver instance) {
    super(instance);
    this.metrics = GobblinMetrics.get(getInstance().getInstanceName());
  }

  @Override
  protected void startUp() throws Exception {
    this.metrics.startMetricReporting(getInstance().getSysConfig().getConfigAsProperties());
  }

  @Override
  protected void shutDown() throws Exception {
    this.metrics.stopMetricsReporting();
    super.shutDown();
  }
}
