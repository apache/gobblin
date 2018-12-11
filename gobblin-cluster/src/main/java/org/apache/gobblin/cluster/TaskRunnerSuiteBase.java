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

package org.apache.gobblin.cluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.task.TaskFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.ConfigUtils;

/**
 * This suite class contains multiple components used by {@link GobblinTaskRunner}.
 * Here is the list of components it contains:
 * A {@link TaskFactory} : register Helix task state model.
 * A {@link MetricContext} : create task related metrics.
 * A {@link StandardMetricsBridge.StandardMetrics} : report task metrics.
 * A list of {@link Service} : register any runtime services necessary to run the tasks.
 */
@Slf4j
@Alpha
public abstract class TaskRunnerSuiteBase {
  protected TaskFactory taskFactory;
  protected GobblinHelixJobFactory jobFactory;
  protected MetricContext metricContext;
  protected String applicationId;
  protected String applicationName;
  protected List<Service> services = Lists.newArrayList();

  protected TaskRunnerSuiteBase(Builder builder) {
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(builder.config), this.getClass());
    this.applicationId = builder.getApplicationId();
    this.applicationName = builder.getApplicationName();
  }

  protected MetricContext getMetricContext() {
    return this.metricContext;
  }

  protected abstract Collection<StandardMetricsBridge.StandardMetrics> getMetricsCollection();

  protected abstract Map<String, TaskFactory> getTaskFactoryMap();

  protected abstract List<Service> getServices();

  protected String getApplicationId() {
    return this.applicationId;
  }

  protected String getApplicationName() {
    return this.applicationName;
  }

  @Getter
  public static class Builder {
    private Config config;
    private HelixManager jobHelixManager;
    private Optional<ContainerMetrics> containerMetrics;
    private FileSystem fs;
    private Path appWorkPath;
    private String applicationId;
    private String applicationName;
    private String instanceName;

    public Builder(Config config) {
      this.config = config;
    }

    public Builder setJobHelixManager(HelixManager jobHelixManager) {
      this.jobHelixManager = jobHelixManager;
      return this;
    }

    public Builder setApplicationName(String applicationName) {
      this.applicationName = applicationName;
      return this;
    }

    public Builder setInstanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }

    public Builder setApplicationId(String applicationId) {
      this.applicationId = applicationId;
      return this;
    }

    public Builder setContainerMetrics(Optional<ContainerMetrics> containerMetrics) {
      this.containerMetrics = containerMetrics;
      return this;
    }

    public Builder setFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public Builder setAppWorkPath(Path appWorkPath) {
      this.appWorkPath = appWorkPath;
      return this;
    }

    public TaskRunnerSuiteBase build() {
      if (getIsRunTaskInSeparateProcessEnabled(config)) {
        return new TaskRunnerSuiteProcessModel(this);
      } else {
        return new TaskRunnerSuiteThreadModel(this);
      }
    }

    private Boolean getIsRunTaskInSeparateProcessEnabled(Config config) {
      return ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.ENABLE_TASK_IN_SEPARATE_PROCESS, false);
    }
  }
}
