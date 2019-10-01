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

package org.apache.gobblin.source.extractor.extract.kafka.workunit.packer;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaUtils;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link KafkaWorkUnitSizeEstimator} which uses the average record size of each partition to
 * estimate the sizes of {@link WorkUnits}.
 *
 * Each partition pulled in the previous run should have an avg record size in its {@link WorkUnitState}. In the
 * next run, for each partition the avg record size pulled in the previous run is considered the avg record size
 * to be pulled in this run.
 *
 * If a partition was not pulled in the previous run, a default value of 1024 is used.
 *
 * @author Ziyang Liu
 */
public class KafkaAvgRecordSizeBasedWorkUnitSizeEstimator implements KafkaWorkUnitSizeEstimator {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvgRecordSizeBasedWorkUnitSizeEstimator.class);

  private static final long DEFAULT_AVG_RECORD_SIZE = 1024;

  private final Map<KafkaPartition, Long> estAvgSizes = Maps.newHashMap();

  public KafkaAvgRecordSizeBasedWorkUnitSizeEstimator(SourceState state) {
    readPreAvgRecordSizes(state);
  }

  @Override
  public double calcEstimatedSize(WorkUnit workUnit) {
    long avgSize = this.getEstAvgSizeForPartition(KafkaUtils.getPartition(workUnit));
    long numOfRecords = workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY)
        - workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
    return (double) avgSize * numOfRecords;
  }

  private long getEstAvgSizeForPartition(KafkaPartition partition) {
    if (this.estAvgSizes.containsKey(partition)) {
      LOG.info(String.format("Estimated avg record size for partition %s is %d", partition,
          this.estAvgSizes.get(partition)));
      return this.estAvgSizes.get(partition);
    }
    LOG.warn(String.format("Avg record size for partition %s not available, using default size %d", partition,
        DEFAULT_AVG_RECORD_SIZE));
    return DEFAULT_AVG_RECORD_SIZE;
  }

  private void readPreAvgRecordSizes(SourceState state) {
    this.estAvgSizes.clear();
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      List<KafkaPartition> partitions = KafkaUtils.getPartitions(workUnitState);
      for (KafkaPartition partition : partitions) {
        if (KafkaUtils.containsPartitionAvgRecordSize(workUnitState, partition)) {
          long previousAvgSize = KafkaUtils.getPartitionAvgRecordSize(workUnitState, partition);
          this.estAvgSizes.put(partition, previousAvgSize);
        }
      }
    }
  }
}
