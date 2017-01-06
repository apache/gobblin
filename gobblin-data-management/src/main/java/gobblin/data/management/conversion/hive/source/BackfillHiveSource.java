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
package gobblin.data.management.conversion.hive.source;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import gobblin.configuration.SourceState;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.WorkUnit;


/**
 * A {@link HiveSource} used to create workunits without a watermark check.
 * {@link #shouldCreateWorkunit(long, LongWatermark)} will always return <code>true</code>
 */
public class BackfillHiveSource extends HiveAvroToOrcSource {

  /**
   * A comma separated list of {@link Partition#getCompleteName()}s that need backfill.
   * If not set, all partitions will be backfilled
   * <p>
   * E.g. service@logEvent@datepartition=2016-08-04-00,service@logEvent@datepartition=2016-08-05-00
   * </p>
   *
   */
  @VisibleForTesting
  public static final String BACKFILL_SOURCE_PARTITION_WHITELIST_KEY = "hive.backfillSource.partitions.whitelist";

  private Set<String> partitionsWhitelist;

  @VisibleForTesting
  public void initBackfillHiveSource(SourceState state) {
    this.partitionsWhitelist =
        Sets.newHashSet(Splitter.on(",").omitEmptyStrings().trimResults().split(state.getProp(BACKFILL_SOURCE_PARTITION_WHITELIST_KEY,
            StringUtils.EMPTY)));
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    initBackfillHiveSource(state);
    return super.getWorkunits(state);
  }

  // Non partitioned tables
  @Override
  public boolean shouldCreateWorkunit(long createTime, long updateTime, LongWatermark lowWatermark) {
    return true;
  }

  //Partitioned tables
  @Override
  public boolean shouldCreateWorkunit(Partition sourcePartition, LongWatermark lowWatermark) {
    // If a whitelist is provided only create workunits for those partitions
    if (!this.partitionsWhitelist.isEmpty()) {
      return this.partitionsWhitelist.contains(sourcePartition.getCompleteName());
    }
    // If no whitelist is set, all partitions of a dataset are backfilled
    return true;
  }

  @Override
  public boolean isOlderThanLookback(Partition partition) {
    // If partition whitelist is provided, ignore lookback
    if (!this.partitionsWhitelist.isEmpty()) {
      return false;
    } else {
      return super.isOlderThanLookback(partition);
    }
  }

}
