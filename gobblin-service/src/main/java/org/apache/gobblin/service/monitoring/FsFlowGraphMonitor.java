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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.MultiHopFlowCompiler;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraphHelper;
import org.apache.gobblin.service.modules.flowgraph.FSPathAlterationFlowGraphListener;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphMonitor;
import org.apache.gobblin.service.modules.template_catalog.UpdatableFSFlowTemplateCatalog;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.filesystem.PathAlterationObserver;
import org.apache.gobblin.util.filesystem.PathAlterationObserverScheduler;


@Slf4j
public class FsFlowGraphMonitor extends AbstractIdleService implements FlowGraphMonitor {
  public static final String FS_FLOWGRAPH_MONITOR_PREFIX = "gobblin.service.fsFlowGraphMonitor";
  public static final String MONITOR_TEMPLATE_CATALOG_CHANGES =  "monitorTemplateChanges";

  private static final long DEFAULT_FLOWGRAPH_POLLING_INTERVAL = 60;
  private static final String DEFAULT_FS_FLOWGRAPH_MONITOR_ABSOLUTE_DIR = "/tmp/fsFlowgraph";
  private static final String DEFAULT_FS_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR = "gobblin-flowgraph";
  private volatile boolean isActive = false;
  private final long pollingInterval;
  private BaseFlowGraphHelper flowGraphHelper;
  private final PathAlterationObserverScheduler pathAlterationDetector;
  private final FSPathAlterationFlowGraphListener listener;
  private final PathAlterationObserver observer;
  private Path flowGraphPath;
  private Path observedPath;
  private final MultiHopFlowCompiler compiler;
  private final CountDownLatch initComplete;
  private static final Config DEFAULT_FALLBACK = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
      .put(ConfigurationKeys.FLOWGRAPH_ABSOLUTE_DIR, DEFAULT_FS_FLOWGRAPH_MONITOR_ABSOLUTE_DIR)
      .put(ConfigurationKeys.FLOWGRAPH_BASE_DIR, DEFAULT_FS_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR)
      .put(ConfigurationKeys.FLOWGRAPH_POLLING_INTERVAL, DEFAULT_FLOWGRAPH_POLLING_INTERVAL)
      .put(ConfigurationKeys.FLOWGRAPH_JAVA_PROPS_EXTENSIONS, ConfigurationKeys.DEFAULT_PROPERTIES_EXTENSIONS)
      .put(MONITOR_TEMPLATE_CATALOG_CHANGES, false)
      .put(ConfigurationKeys.FLOWGRAPH_HOCON_FILE_EXTENSIONS, ConfigurationKeys.DEFAULT_CONF_EXTENSIONS).build());

  public FsFlowGraphMonitor(Config config, Optional<UpdatableFSFlowTemplateCatalog> flowTemplateCatalog,
      MultiHopFlowCompiler compiler, Map<URI, TopologySpec> topologySpecMap, CountDownLatch initComplete, boolean instrumentationEnabled)
      throws IOException {
    Config configWithFallbacks = config.getConfig(FS_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK);
    this.pollingInterval =
        TimeUnit.SECONDS.toMillis(configWithFallbacks.getLong(ConfigurationKeys.FLOWGRAPH_POLLING_INTERVAL));
    this.flowGraphPath = new Path(configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_ABSOLUTE_DIR));
    // If the FSFlowGraphMonitor is also monitoring the templates, assume that they are colocated under the same parent folder
    boolean shouldMonitorTemplateCatalog = configWithFallbacks.getBoolean(MONITOR_TEMPLATE_CATALOG_CHANGES);
    this.observedPath = shouldMonitorTemplateCatalog ? this.flowGraphPath.getParent() : this.flowGraphPath;
    this.observer = new PathAlterationObserver(observedPath);
    try {
      String helperClassName = ConfigUtils.getString(config, ServiceConfigKeys.GOBBLIN_SERVICE_FLOWGRAPH_HELPER_KEY,
          BaseFlowGraphHelper.class.getCanonicalName());
      this.flowGraphHelper = (BaseFlowGraphHelper) ConstructorUtils.invokeConstructor(Class.forName(new ClassAliasResolver<>(BaseFlowGraphHelper.class).resolve(helperClassName)), flowTemplateCatalog, topologySpecMap, flowGraphPath.toString(),
          configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_BASE_DIR), configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_JAVA_PROPS_EXTENSIONS),
          configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_HOCON_FILE_EXTENSIONS), instrumentationEnabled, config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.listener = new FSPathAlterationFlowGraphListener(flowTemplateCatalog, compiler, flowGraphPath.toString(), this.flowGraphHelper, shouldMonitorTemplateCatalog);
    this.compiler = compiler;
    this.initComplete = initComplete;

    if (pollingInterval == ConfigurationKeys.DISABLED_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL) {
      this.pathAlterationDetector = null;
    } else {
      this.pathAlterationDetector = new PathAlterationObserverScheduler(pollingInterval);
      Optional<PathAlterationObserver> observerOptional = Optional.fromNullable(observer);
      this.pathAlterationDetector.addPathAlterationObserver(this.listener, observerOptional,
          this.observedPath);
    }
  }

  @Override
  protected void startUp()
      throws IOException {
  }

  @Override
  public synchronized void setActive(boolean isActive) {
    log.info("Setting the flow graph monitor to be " + isActive + " from " + this.isActive);
    if (this.isActive == isActive) {
      // No-op if already in correct state
      return;
    } else if (isActive) {
      if (this.pathAlterationDetector != null) {
        log.info("Starting the " + getClass().getSimpleName());
        log.info("Polling folder {} with interval {} ", this.observedPath, this.pollingInterval);
        try {
          this.pathAlterationDetector.start();
          // Manually instantiate flowgraph when the monitor becomes active
          this.compiler.setFlowGraph(this.flowGraphHelper.generateFlowGraph());
          // Reduce the countdown latch
          this.initComplete.countDown();
          log.info("Finished populating FSFlowgraph");
        } catch (IOException e) {
          log.error("Could not initialize pathAlterationDetector due to error: ", e);
        }
      } else {
        log.warn("No path alteration detector found");
      }
    }
    this.isActive = isActive;
  }

  /** Stop the service. */
  @Override
  protected void shutDown()
      throws Exception {
    this.pathAlterationDetector.stop();
  }
}
