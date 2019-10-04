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

package org.apache.gobblin.data.management.retention.source;

import java.util.List;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.retention.StateCleanerTaskFactory;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;


public class StateCleanerSource implements Source<Object, Object> {
  public static final String STATE_CLEANER_SOURCE_PREFIX = "stateCleanerSource";
  public static final String STATE_CLEANER_CONFIGURATIONS = STATE_CLEANER_SOURCE_PREFIX + ".configurations";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    Config config = ConfigUtils.propertiesToConfig(state.getProperties());
    Config sourceConfig = ConfigUtils.getConfigOrEmpty(config, STATE_CLEANER_SOURCE_PREFIX);
    List<String> configurationNames = ConfigUtils.getStringList(config, STATE_CLEANER_CONFIGURATIONS);

    // use a dummy configuration name if none set
    if (configurationNames.isEmpty()) {
      configurationNames = ImmutableList.of("DummyConfig");
    }

    for (String configurationName: configurationNames) {
      WorkUnit workUnit = WorkUnit.createEmpty();

      // specific configuration prefixed by the configuration name has precedence over the source specific configuration
      // and the source specific configuration has precedence over the general configuration
      Config wuConfig = ConfigUtils.getConfigOrEmpty(sourceConfig, configurationName).withFallback(sourceConfig)
          .withFallback(config);

      workUnit.setProps(ConfigUtils.configToProperties(wuConfig), new Properties());
      TaskUtils.setTaskFactoryClass(workUnit, StateCleanerTaskFactory.class);
      workUnits.add(workUnit);
    }

    return workUnits;
  }

  @Override
  public Extractor<Object, Object> getExtractor(WorkUnitState state) {
    return null;
  }

  @Override
  public void shutdown(SourceState state) {
  }
}
