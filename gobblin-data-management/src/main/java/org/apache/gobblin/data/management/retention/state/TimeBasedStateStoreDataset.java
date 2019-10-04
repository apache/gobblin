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

package org.apache.gobblin.data.management.retention.state;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import lombok.Data;

import org.apache.gobblin.data.management.policy.SelectBeforeTimeBasedPolicy;
import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;
import org.apache.gobblin.data.management.version.TimestampedStateStoreVersion;
import org.apache.gobblin.data.management.version.finder.TimestampedStateStoreVersionFinder;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.metastore.StateStoreDataset;
import org.apache.gobblin.metastore.metadata.StateStoreEntryManager;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A {@link CleanableStateStoreDataset} that deletes entries before a certain time
 */

@Data
public class TimeBasedStateStoreDataset extends CleanableStateStoreDataset<TimestampedDatasetVersion> {
  private static final String SELECTION_POLICY_CLASS_KEY = "selection.policy.class";
  private static final String DEFAULT_SELECTION_POLICY_CLASS = SelectBeforeTimeBasedPolicy.class.getName();

  private final VersionFinder<TimestampedStateStoreVersion> versionFinder;
  private final VersionSelectionPolicy<TimestampedDatasetVersion> versionSelectionPolicy;

  public TimeBasedStateStoreDataset(StateStoreDataset.Key key, List<StateStoreEntryManager> entries, Properties props) {
    super(key, entries);
    this.versionFinder = new TimestampedStateStoreVersionFinder();
    Config propsAsConfig = ConfigUtils.propertiesToConfig(props);

    // strip the retention config namespace since the selection policy looks for configuration without the namespace
    Config retentionConfig = ConfigUtils.getConfigOrEmpty(propsAsConfig,
        ConfigurableCleanableDataset.RETENTION_CONFIGURATION_KEY);
    Config retentionConfigWithFallback = retentionConfig.withFallback(propsAsConfig);

    this.versionSelectionPolicy = createSelectionPolicy(ConfigUtils.getString(retentionConfigWithFallback,
        SELECTION_POLICY_CLASS_KEY, DEFAULT_SELECTION_POLICY_CLASS), retentionConfigWithFallback, props);
  }

  public VersionFinder<TimestampedStateStoreVersion> getVersionFinder() {
    return this.versionFinder;
  }

  public VersionSelectionPolicy<TimestampedDatasetVersion> getVersionSelectionPolicy() {
    return this.versionSelectionPolicy;
  }

  @SuppressWarnings("unchecked")
  private VersionSelectionPolicy<TimestampedDatasetVersion> createSelectionPolicy(String className,
      Config config, Properties jobProps) {
    try {
      return (VersionSelectionPolicy<TimestampedDatasetVersion>)
          GobblinConstructorUtils.invokeFirstConstructor(Class.forName(className),
          ImmutableList.of(config), ImmutableList.of(config, jobProps),
          ImmutableList.of(jobProps));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
