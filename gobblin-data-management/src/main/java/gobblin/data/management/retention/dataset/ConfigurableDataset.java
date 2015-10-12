/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.finder.VersionFinder;


/**
 * {@link gobblin.data.management.retention.dataset.DatasetBase} that instantiates {@link VersionFinder} and
 * {@link RetentionPolicy} from classes read from an input {@link java.util.Properties}.
 *
 * <p>
 *   The class of {@link VersionFinder} should be under key {@link #VERSION_FINDER_CLASS_KEY}, while the class of
 *   {@link RetentionPolicy} should be under key {@link #RETENTION_POLICY_CLASS_KEY}.
 * </p>
 */
public class ConfigurableDataset<T extends DatasetVersion> extends DatasetBase<T> {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String VERSION_FINDER_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "version.finder.class";
  public static final String RETENTION_POLICY_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "retention.policy.class";

  private final Path datasetRoot;

  private final VersionFinder<? extends T> versionFinder;
  private final RetentionPolicy<T> retentionPolicy;

  public ConfigurableDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(ConfigurableDataset.class));
  }

  @SuppressWarnings("unchecked")
  public ConfigurableDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log)
      throws IOException {
    super(fs, props, log);
    this.datasetRoot = datasetRoot;

    Preconditions.checkArgument(props.containsKey(VERSION_FINDER_CLASS_KEY),
        "Missing property " + VERSION_FINDER_CLASS_KEY);
    Preconditions.checkArgument(props.containsKey(RETENTION_POLICY_CLASS_KEY),
        "Missing property " + RETENTION_POLICY_CLASS_KEY);

    try {
      Class<?> versionFinderClass = Class.forName(props.getProperty(VERSION_FINDER_CLASS_KEY));
      Class<?> retentionPolicyClass = Class.forName(props.getProperty(RETENTION_POLICY_CLASS_KEY));
      this.versionFinder = (VersionFinder) versionFinderClass.
          getConstructor(FileSystem.class, Properties.class).newInstance(this.fs, props);
      this.retentionPolicy = (RetentionPolicy) retentionPolicyClass.
          getConstructor(Properties.class).newInstance(props);
    } catch(ClassNotFoundException exception) {
      throw new IOException(exception);
    } catch(NoSuchMethodException exception) {
      throw new IOException(exception);
    } catch(InstantiationException exception) {
      throw new IOException(exception);
    } catch(IllegalAccessException exception) {
      throw new IOException(exception);
    } catch(InvocationTargetException exception) {
      throw new IOException(exception);
    }
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
