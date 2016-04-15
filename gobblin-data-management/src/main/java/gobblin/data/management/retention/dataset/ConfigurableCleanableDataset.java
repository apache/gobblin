/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import gobblin.data.management.policy.VersionSelectionPolicy;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.finder.VersionFinder;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * {@link CleanableDatasetBase} that instantiates {@link VersionFinder} and {@link RetentionPolicy} from classes read
 * from an input {@link java.util.Properties}.
 *
 * <p>
 * The class of {@link VersionFinder} should be under key {@link #VERSION_FINDER_CLASS_KEY}, while the class of
 * {@link RetentionPolicy} should be under key {@link #RETENTION_POLICY_CLASS_KEY}.
 * </p>
 */
public class ConfigurableCleanableDataset<T extends FileSystemDatasetVersion> extends
    MultiVersionCleanableDatasetBase<T> {

  public static final String RETENTION_CONFIGURATION_KEY = "gobblin.retention";
  public static final String CONFIGURATION_KEY_PREFIX = RETENTION_CONFIGURATION_KEY + ".";
  public static final String VERSION_FINDER_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "version.finder.class";
  public static final String RETENTION_POLICY_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "retention.policy.class";
  public static final String SELECTION_POLICY_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "selection.policy.class";

  /**
   * This key is used if the dataset contains multiple partition each with its own version finder and selection policy.
   *
   * gobblin.retention.dataset.partitions is a list of version finder and policies.
   *
   * E.g.
   * <pre>
   *
   * gobblin.retention {
   *    partitions : [
   *    {
   *      selection {
   *            policy.class = data.management.policy.SelectBeforeTimeBasedPolicy
   *            timeBased.lookbackTime = 5d
   *       }
   *       version : {
   *           finder.class=gobblin.data.management.version.finder.DateTimeDatasetVersionFinder
   *           pattern="hourly/*&#47;"
   *       }
   *    },
   *    {
   *      selection {
   *           policy.class = data.management.policy.SelectBeforeTimeBasedPolicy
   *           timeBased.lookbackTime = 20d
   *       }
   *       version : {
   *           finder.class=gobblin.data.management.version.finder.DateTimeDatasetVersionFinder
   *           pattern="daily/*&#47;"
   *       }
   *    }
   *    ]
   * }
   *
   *
   * </pre>
   */
  public static final String DATASET_PARTITIONS_LIST_KEY = CONFIGURATION_KEY_PREFIX + "dataset.partitions";

  private final Path datasetRoot;
  private final List<VersionFinderAndPolicy<T>> versionFindersAndPolicies;

  /**
   * Creates a new ConfigurableCleanableDataset configured through gobblin-config-management. The constructor expects
   * {@link #VERSION_FINDER_CLASS_KEY} and {@link #RETENTION_POLICY_CLASS_KEY} to be available in the
   * <code>config</code> passed.
   */
  public ConfigurableCleanableDataset(FileSystem fs, Properties jobProps, Path datasetRoot, Config config, Logger log)
      throws IOException {

    super(fs, jobProps, log);
    this.datasetRoot = datasetRoot;
    this.versionFindersAndPolicies = Lists.newArrayList();

    if (config.hasPath(VERSION_FINDER_CLASS_KEY) && config.hasPath(RETENTION_POLICY_CLASS_KEY)) {
      initWithRetentionPolicy(config, jobProps, RETENTION_POLICY_CLASS_KEY, VERSION_FINDER_CLASS_KEY);
    } else if (config.hasPath(VERSION_FINDER_CLASS_KEY) && config.hasPath(SELECTION_POLICY_CLASS_KEY)) {
      initWithSelectionPolicy(config.getConfig(RETENTION_CONFIGURATION_KEY), jobProps);
    } else if (config.hasPath(DATASET_PARTITIONS_LIST_KEY)) {
      List<? extends Config> versionAndPolicies = config.getConfigList(DATASET_PARTITIONS_LIST_KEY);
      for (Config versionAndPolicy : versionAndPolicies) {
        initWithSelectionPolicy(versionAndPolicy, jobProps);
      }
    } else {
      throw new IllegalArgumentException(String.format(
          "Either set version finder at %s and retention policy at %s or set partitions at %s",
          VERSION_FINDER_CLASS_KEY, RETENTION_POLICY_CLASS_KEY, DATASET_PARTITIONS_LIST_KEY));
    }
  }

  public ConfigurableCleanableDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(ConfigurableCleanableDataset.class));
  }

  public ConfigurableCleanableDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log) throws IOException {
    this(fs, props, datasetRoot, ConfigFactory.parseProperties(props), log);
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }

  @Override
  public List<VersionFinderAndPolicy<T>> getVersionFindersAndPolicies() {
    return this.versionFindersAndPolicies;
  }

  private void initWithRetentionPolicy(Config config, Properties jobProps, String retentionPolicyKey,
      String versionFinderKey) {
    this.versionFindersAndPolicies.add(new VersionFinderAndPolicy<>(createRetentionPolicy(
        config.getString(retentionPolicyKey), config, jobProps), createVersionFinder(
        config.getString(versionFinderKey), config, jobProps)));
  }

  private void initWithSelectionPolicy(Config config, Properties jobProps) {

    String selectionPolicyKey = StringUtils.substringAfter(SELECTION_POLICY_CLASS_KEY, CONFIGURATION_KEY_PREFIX);
    String versionFinderKey = StringUtils.substringAfter(VERSION_FINDER_CLASS_KEY, CONFIGURATION_KEY_PREFIX);

    if (config.hasPath(selectionPolicyKey) && config.hasPath(versionFinderKey)) {
      this.versionFindersAndPolicies.add(new VersionFinderAndPolicy<>(createSelectionPolicy(
          config.getString(selectionPolicyKey), config, jobProps), createVersionFinder(
          config.getString(versionFinderKey), config, jobProps)));
    } else {
      log.warn(String
          .format(
              "Version finder and policy not initialized for partition. Set version finder at %s and Selection policy at %s",
              VERSION_FINDER_CLASS_KEY, SELECTION_POLICY_CLASS_KEY));
    }
  }

  @SuppressWarnings("unchecked")
  private VersionFinder<? extends T> createVersionFinder(String className, Config config, Properties jobProps) {
    try {
      return (VersionFinder<? extends T>) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(className),
          ImmutableList.<Object> of(this.fs, config), ImmutableList.<Object> of(this.fs, jobProps));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private RetentionPolicy<T> createRetentionPolicy(String className, Config config, Properties jobProps) {
    try {
      return (RetentionPolicy<T>) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(className),
          ImmutableList.<Object> of(config), ImmutableList.<Object> of(config, jobProps),
          ImmutableList.<Object> of(jobProps));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private VersionSelectionPolicy<T> createSelectionPolicy(String className, Config config, Properties jobProps) {
    try {
      log.info(String.format("Configuring selection policy %s for %s with %s", className, this.datasetRoot,
                    config.root().render(ConfigRenderOptions.concise())));
      return (VersionSelectionPolicy<T>) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(className),
          ImmutableList.<Object> of(config), ImmutableList.<Object> of(config, jobProps),
          ImmutableList.<Object> of(jobProps));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
