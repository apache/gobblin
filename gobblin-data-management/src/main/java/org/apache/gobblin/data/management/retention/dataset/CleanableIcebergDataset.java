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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.iceberg.GobblinMCEProducer;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A subclass of {@link ConfigurableCleanableDataset} that overwrite the {@link ConfigurableCleanableDataset#cleanImpl(Collection)}
 * to firstly send gmce to delete dataset logically from iceberg and then process GMCEs within metadata-ingestion pipeline
 * by calling {@link org.apache.iceberg.Table#expireSnapshots()} to materialize data/metadata retention
 */
public class CleanableIcebergDataset<T extends FileSystemDatasetVersion> extends ConfigurableCleanableDataset<T> {
  private final static String RETENTION_INTERVAL_TIME = "retention.interval.time";
  private final static String DEFAULT_RETENTION_INTERVAL_TIME = "10000";
  protected Config config;
  protected Properties jobProps;
  Set<TableIdentifier> expiredTable;

  public CleanableIcebergDataset(FileSystem fs, Properties jobProps, Path datasetRoot, Config config, Logger log)
      throws IOException {
    super(fs, jobProps, datasetRoot, config, log);
    this.config = config;
    this.jobProps = jobProps;
    this.expiredTable = new HashSet<>();
  }

  public CleanableIcebergDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(CleanableIcebergDataset.class));
  }

  public CleanableIcebergDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log) throws IOException {
    this(fs, props, datasetRoot, ConfigFactory.parseProperties(props), log);
  }

  @Override
  public void clean() throws IOException {

    if (this.isDatasetBlacklisted) {
      this.log.info("Dataset blacklisted. Cleanup skipped for " + datasetRoot());
      return;
    }

    boolean atLeastOneFailureSeen = false;

    for (VersionFinderAndPolicy<T> versionFinderAndPolicy : getVersionFindersAndPolicies()) {
      Config retentionConfig = versionFinderAndPolicy.getConfig();
      Preconditions.checkArgument(retentionConfig != null,
          "Must specify retention config for iceberg dataset retention");
      VersionSelectionPolicy<T> selectionPolicy = versionFinderAndPolicy.getVersionSelectionPolicy();
      VersionFinder<? extends T> versionFinder = versionFinderAndPolicy.getVersionFinder();

      if (!selectionPolicy.versionClass().isAssignableFrom(versionFinder.versionClass())) {
        throw new IOException("Incompatible dataset version classes.");
      }

      this.log.info(String.format("Cleaning dataset %s. Using version finder %s and policy %s", this,
          versionFinder.getClass().getName(), selectionPolicy));

      List<T> versions = Lists.newArrayList(versionFinder.findDatasetVersions(this));

      if (versions.isEmpty()) {
        this.log.warn("No dataset version can be found. Ignoring.");
        continue;
      }

      Collections.sort(versions, Collections.reverseOrder());

      Collection<T> deletableVersions = selectionPolicy.listSelectedVersions(versions);

      cleanImpl(deletableVersions, retentionConfig);
    }

    if (atLeastOneFailureSeen) {
      throw new RuntimeException(
          String.format("At least one failure happened while processing %s. Look for previous logs for failures",
              datasetRoot()));
    }
    try {
      // Sleep for a while to make sure metadata pipeline won't get bunch of retention events at the same time, which will
      // affect the SLA of retention pipeline
      Thread.sleep(Long.parseLong(jobProps.getProperty(RETENTION_INTERVAL_TIME, DEFAULT_RETENTION_INTERVAL_TIME)));
    } catch (InterruptedException e) {
      log.error("interrupted while sleep");
      throw new IOException(e);
    }
  }

  /**
   * Only in charge of filing {@link org.apache.gobblin.metadata.GobblinMetadataChangeEvent}
   * The processing of these events can be seen in {@link org.apache.gobblin.iceberg.writer.IcebergMetadataWriter}.
   */
  protected void cleanImpl(Collection<T> deletableVersions, Config retentionConfig) throws IOException {
    List<String> deletablePrefix = new ArrayList<>();
    for (T version : deletableVersions) {
      version.getPaths().forEach(p -> deletablePrefix.add(fs.makeQualified(p).toString()));
    }
    if (deletablePrefix.isEmpty()) {
      return;
    }
    Preconditions.checkArgument(retentionConfig.hasPath(ConfigurationKeys.HIVE_REGISTRATION_POLICY));
    Preconditions.checkArgument(retentionConfig.hasPath(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME));

    Properties prop = new Properties();
    prop.putAll(jobProps);
    State producerState = new State(prop);
    producerState.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY,
        retentionConfig.getString(ConfigurationKeys.HIVE_REGISTRATION_POLICY));
    producerState.setProp(GobblinMCEProducer.OLD_FILES_HIVE_REGISTRATION_KEY,
        retentionConfig.getString(ConfigurationKeys.HIVE_REGISTRATION_POLICY));
    producerState.setProp(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME,
        retentionConfig.getString(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME));
    if (retentionConfig.hasPath(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES)) {
      producerState.setProp(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES,
          retentionConfig.getString(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES));
    }
    producerState.setProp(ConfigurationKeys.DATA_PUBLISHER_DATASET_DIR, this.datasetURN());
    if (!this.simulate) {
      try (GobblinMCEProducer producer = GobblinMCEProducer.getGobblinMCEProducer(producerState)) {
        producer.sendGMCE(null, null, deletablePrefix, null, OperationType.drop_files, SchemaSource.NONE);
        log.info("Sent gmce to delete path {} from icebergTable",
            deletablePrefix.stream().map(Object::toString).collect(Collectors.joining(",")));
      }
    } else {
      log.info("In simulate mode, going to send gmce to delete path {} from icebergTable",
          deletablePrefix.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
  }
}
