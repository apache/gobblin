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
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.finder.VersionFinder;


/**
 * {@link CleanableDatasetBase} that instantiates {@link VersionFinder} and {@link RetentionPolicy} from classes read
 * from an input {@link java.util.Properties}.
 *
 * <p>
 * The class of {@link VersionFinder} should be under key {@link #VERSION_FINDER_CLASS_KEY}, while the class of
 * {@link RetentionPolicy} should be under key {@link #RETENTION_POLICY_CLASS_KEY}.
 * </p>
 */
public class ConfigurableCleanableDataset<T extends DatasetVersion> extends CleanableDatasetBase<T> {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String VERSION_FINDER_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "version.finder.class";
  public static final String RETENTION_POLICY_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "retention.policy.class";

  private final Path datasetRoot;

  private final VersionFinder<? extends T> versionFinder;
  private final RetentionPolicy<T> retentionPolicy;

  /**
   * Creates a new ConfigurableCleanableDataset configured through gobblin-config-management. The constructor expects
   * {@link #VERSION_FINDER_CLASS_KEY} and {@link #RETENTION_POLICY_CLASS_KEY} to be available in the
   * <code>config</code> passed.
   */
  @SuppressWarnings("unchecked")
  public ConfigurableCleanableDataset(FileSystem fs, Properties jobProps, Path datasetRoot, Config config, Logger log)
      throws IOException {

    super(fs, jobProps, log);
    this.datasetRoot = datasetRoot;

    Preconditions.checkArgument(config.hasPath(VERSION_FINDER_CLASS_KEY), "Missing property " + VERSION_FINDER_CLASS_KEY);
    Preconditions.checkArgument(config.hasPath(RETENTION_POLICY_CLASS_KEY), "Missing property " + RETENTION_POLICY_CLASS_KEY);

    try {
      this.versionFinder =
          (VersionFinder<? extends T>) ConstructorUtils.invokeConstructor(Class.forName(config.getString(VERSION_FINDER_CLASS_KEY)),
              this.fs, config);
      this.retentionPolicy =
          (RetentionPolicy<T>) ConstructorUtils.invokeConstructor(Class.forName(config.getString(RETENTION_POLICY_CLASS_KEY)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public ConfigurableCleanableDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(ConfigurableCleanableDataset.class));
  }

  public ConfigurableCleanableDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log) throws IOException {
    this(fs, props, datasetRoot, ConfigFactory.parseProperties(props), log);
  }

  @Override
  public VersionFinder<? extends T> getVersionFinder() {
    return this.versionFinder;
  }

  @Override
  public RetentionPolicy<T> getRetentionPolicy() {
    return this.retentionPolicy;
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }
}
