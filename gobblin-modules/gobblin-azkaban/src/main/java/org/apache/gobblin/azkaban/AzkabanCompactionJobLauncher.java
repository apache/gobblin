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

package org.apache.gobblin.azkaban;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import azkaban.jobExecutor.AbstractJob;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import org.apache.log4j.Logger;

import org.apache.gobblin.compaction.Compactor;
import org.apache.gobblin.compaction.CompactorFactory;
import org.apache.gobblin.compaction.CompactorCreationException;
import org.apache.gobblin.compaction.listeners.CompactorListener;
import org.apache.gobblin.compaction.listeners.CompactorListenerCreationException;
import org.apache.gobblin.compaction.listeners.CompactorListenerFactory;
import org.apache.gobblin.compaction.ReflectionCompactorFactory;
import org.apache.gobblin.compaction.listeners.ReflectionCompactorListenerFactory;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A class for launching a Gobblin MR job for compaction through Azkaban.
 * @deprecated use {@link AzkabanJobLauncher} and {@link org.apache.gobblin.compaction.source.CompactionSource}
 */
@Deprecated
public class AzkabanCompactionJobLauncher extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanCompactionJobLauncher.class);

  private final Properties properties;
  private final Compactor compactor;

  public AzkabanCompactionJobLauncher(String jobId, Properties props) {
    super(jobId, LOG);
    this.properties = new Properties();
    this.properties.putAll(props);

    // load dynamic configuration and add them to the job properties
    Config propsAsConfig = ConfigUtils.propertiesToConfig(props);
    DynamicConfigGenerator dynamicConfigGenerator =
        DynamicConfigGeneratorFactory.createDynamicConfigGenerator(propsAsConfig);
    Config dynamicConfig = dynamicConfigGenerator.generateDynamicConfig(propsAsConfig);

    // add the dynamic config to the job config
    for (Map.Entry<String, ConfigValue> entry : dynamicConfig.entrySet()) {
      this.properties.put(entry.getKey(), entry.getValue().unwrapped().toString());
    }

    this.compactor = getCompactor(getCompactorFactory(), getCompactorListener(getCompactorListenerFactory()));
  }

  private Compactor getCompactor(CompactorFactory compactorFactory, Optional<CompactorListener> compactorListener) {
    try {
      return compactorFactory
          .createCompactor(this.properties, Tag.fromMap(AzkabanTags.getAzkabanTags()), compactorListener);
    } catch (CompactorCreationException e) {
      throw new RuntimeException("Unable to create compactor", e);
    }
  }

  protected CompactorFactory getCompactorFactory() {
    return new ReflectionCompactorFactory();
  }

  private Optional<CompactorListener> getCompactorListener(CompactorListenerFactory compactorListenerFactory) {
    try {
      return compactorListenerFactory.createCompactorListener(this.properties);
    } catch (CompactorListenerCreationException e) {
      throw new RuntimeException("Unable to create compactor listener", e);
    }
  }

  protected CompactorListenerFactory getCompactorListenerFactory() {
    return new ReflectionCompactorListenerFactory();
  }

  @Override
  public void run() throws Exception {
    this.compactor.compact();
  }

  @Override
  public void cancel() throws IOException {
    this.compactor.cancel();
  }
}
