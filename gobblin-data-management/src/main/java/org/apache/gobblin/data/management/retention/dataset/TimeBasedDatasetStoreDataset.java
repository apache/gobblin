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

package org.apache.gobblin.data.management.retention.dataset;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import org.apache.gobblin.data.management.policy.SelectBeforeTimeBasedPolicy;
import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.version.TimestampedDatasetStateStoreVersion;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;
import org.apache.gobblin.data.management.version.finder.TimestampedDatasetStateStoreVersionFinder;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import lombok.Data;


/**
 * A {@link CleanableDatasetStoreDataset} that deletes entries before a certain time
 */
@Data
public class TimeBasedDatasetStoreDataset extends CleanableDatasetStoreDataset<TimestampedDatasetVersion> {
  private static final String SELECTION_POLICY_CLASS_KEY = "selection.policy.class";
  private static final String DEFAULT_SELECTION_POLICY_CLASS = SelectBeforeTimeBasedPolicy.class.getName();

  private final VersionFinder<TimestampedDatasetStateStoreVersion> versionFinder;
  private final VersionSelectionPolicy<TimestampedDatasetVersion> versionSelectionPolicy;

  public TimeBasedDatasetStoreDataset(Key key, List<DatasetStateStoreEntryManager> entries, Properties props) {
    super(key, entries);
    this.versionFinder = new TimestampedDatasetStateStoreVersionFinder();
    Config propsAsConfig = ConfigUtils.propertiesToConfig(props);

    // strip the retention config namespace since the selection policy looks for configuration without the namespace
    Config retentionConfig = ConfigUtils.getConfigOrEmpty(propsAsConfig,
        ConfigurableCleanableDataset.RETENTION_CONFIGURATION_KEY);
    Config retentionConfigWithFallback = retentionConfig.withFallback(propsAsConfig);

    this.versionSelectionPolicy = createSelectionPolicy(ConfigUtils.getString(retentionConfigWithFallback,
        SELECTION_POLICY_CLASS_KEY, DEFAULT_SELECTION_POLICY_CLASS), retentionConfigWithFallback, props);
  }

  @Override
  public VersionFinder<TimestampedDatasetStateStoreVersion> getVersionFinder() {
    return this.versionFinder;
  }

  @Override
  public VersionSelectionPolicy<TimestampedDatasetVersion> getVersionSelectionPolicy() {
    return this.versionSelectionPolicy;
  }

  @SuppressWarnings("unchecked")
  private VersionSelectionPolicy<TimestampedDatasetVersion> createSelectionPolicy(String className,
      Config config, Properties jobProps) {
    try {
      return (VersionSelectionPolicy<TimestampedDatasetVersion>)
          GobblinConstructorUtils.invokeFirstConstructor(Class.forName(className),
          ImmutableList.<Object> of(config), ImmutableList.<Object> of(config, jobProps),
          ImmutableList.<Object> of(jobProps));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
