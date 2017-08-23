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
package org.apache.gobblin.data.management.retention;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import azkaban.jobExecutor.AbstractJob;

import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDatasetFinder;
import org.apache.gobblin.data.management.conversion.hive.events.EventConstants;
import org.apache.gobblin.data.management.conversion.hive.validation.ValidationJob;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.HiveUtils;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.ConfigUtils;


public class Avro2OrcStaleDatasetCleaner extends AbstractJob {
  private static final Logger log = Logger.getLogger(ValidationJob.class);
  private static final String HIVE_PARTITION_DELETION_GRACE_TIME_IN_DAYS = "hive.partition.deletion.graceTime.inDays";
  private static final String DEFAULT_HIVE_PARTITION_DELETION_GRACE_TIME_IN_DAYS = "2";
  private final MetricContext metricContext;
  private final EventSubmitter eventSubmitter;
  private final ConvertibleHiveDatasetFinder datasetFinder;
  private static final String HIVE_DATASET_CONFIG_AVRO_PREFIX = "hive.conversion.avro";
  private final FileSystem fs;
  private final long graceTimeInMillis;

  public Avro2OrcStaleDatasetCleaner(String jobId, Properties props)
      throws IOException {
    super(jobId, log);
    props.setProperty(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, HIVE_DATASET_CONFIG_AVRO_PREFIX);
    this.graceTimeInMillis = TimeUnit.DAYS.toMillis(Long.parseLong(props
        .getProperty(HIVE_PARTITION_DELETION_GRACE_TIME_IN_DAYS, DEFAULT_HIVE_PARTITION_DELETION_GRACE_TIME_IN_DAYS)));
    Config config = ConfigFactory.parseProperties(props);
    this.fs = FileSystem.newInstance(new Configuration());
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), ValidationJob.class);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, EventConstants.CONVERSION_NAMESPACE).build();
    this.datasetFinder = new ConvertibleHiveDatasetFinder(this.fs, props, this.eventSubmitter);
  }

  @Override
  public void run()
      throws Exception {
    Iterator<HiveDataset> iterator = this.datasetFinder.getDatasetsIterator();
    while (iterator.hasNext()) {
      ConvertibleHiveDataset hiveDataset = (ConvertibleHiveDataset) iterator.next();
      try (AutoReturnableObject<IMetaStoreClient> client = hiveDataset.getClientPool().getClient()) {
        Set<Partition> sourcePartitions =
            new HashSet<>(HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), Optional.<String>absent()));

        sourcePartitions.parallelStream().filter(partition -> isUnixTimeStamp(partition.getDataLocation().getName()))
            .forEach(partition -> {
              Arrays.stream(listFiles(partition.getDataLocation().getParent())).filter(
                  fileStatus -> !fileStatus.getPath().toString()
                      .equalsIgnoreCase(partition.getDataLocation().toString())).forEach(fileStatus -> {
                deletePath(fileStatus, this.graceTimeInMillis, true);
              });
            });
      }
    }
  }

  private FileStatus[] listFiles(Path path) {
    try {
      return this.fs.listStatus(path);
    } catch (IOException e) {
      log.error("Unalbe to list files for directory " + path, e);
      return new FileStatus[0];
    }
  }

  private void deletePath(FileStatus fileStatus, long graceTimeInMillis, boolean recursively) {
    long modificationTime = fileStatus.getModificationTime();
    long currentTime = System.currentTimeMillis();
    if ((currentTime - modificationTime) < 0) {
      log.error("Modification time cannot be greater than current time: " + fileStatus.getPath());
      return;
    }
    if ((currentTime - modificationTime) < graceTimeInMillis) {
      log.info("Modification time is still within grace time for deletion: " + fileStatus.getPath());
      return;
    }
    try {
      this.fs.delete(fileStatus.getPath(), recursively);
      log.info("Deleted path " + fileStatus.getPath());
    } catch (IOException e) {
      log.error("Unable to delete directory " + fileStatus.getPath(), e);
    }
  }

  /**
   * Check if a given string is a valid unixTimeStamp
   */
  private static boolean isUnixTimeStamp(String timeStamp) {
    int TIME_STAMP_LENGTH = 13;
    if (timeStamp.length() != TIME_STAMP_LENGTH) {
      return false;
    }
    try {
      Long.parseLong(timeStamp);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
