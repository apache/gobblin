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
package org.apache.gobblin.writer.partitioner;

import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;


public class CustomPartitionerForMigrationByTimestamp extends TimeBasedAvroWriterPartitioner {
  public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";
  protected String writerPartitionPrefixBackup;
  public boolean localConsumptionProgress;
  public long localConsumptionCutoverUnixTime;
  public String localConsumptionPipelineType;

  public CustomPartitionerForMigrationByTimestamp(State state) {
    this(state, 1, 0);
  }

  public CustomPartitionerForMigrationByTimestamp(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.localConsumptionProgress = state.getPropAsBoolean(ConfigurationKeys.LOCAL_CONSUMPTION_ON, false);
    if (this.localConsumptionProgress) {
      Preconditions.checkNotNull(state.getProp(ConfigurationKeys.LOCAL_CONSUMPTION_CUTOVER_UNIX));
      Preconditions.checkNotNull(state.getProp(ConfigurationKeys.LOCAL_CONSUMPTION_PIPELINE_TYPE));
      Preconditions.checkNotNull(state.getProp(ConfigurationKeys.LOCAL_CONSUMPTION_WRITER_PARTITION_PREFIX));
      this.localConsumptionCutoverUnixTime = state.getPropAsLong(ConfigurationKeys.LOCAL_CONSUMPTION_CUTOVER_UNIX);
      this.localConsumptionPipelineType = state.getProp(ConfigurationKeys.LOCAL_CONSUMPTION_PIPELINE_TYPE);
      this.writerPartitionPrefixBackup = state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.LOCAL_CONSUMPTION_WRITER_PARTITION_PREFIX, numBranches, branchId));
    }
  }

  @Override
  public GenericRecord partitionForRecord(GenericRecord record) {
    long timestamp = timeUnit.toMillis(getRecordTimestamp(record));
    GenericRecord partition = super.partitionForRecord(record);

    if (this.localConsumptionProgress) {
      // Only use backup prefix for agg pipeline when record time is past cutover time
      // Only use backup prefix for local pipeline when record time is prior to cutover time
      if ((this.localConsumptionPipelineType.equals("aggregate") && timestamp >= this.localConsumptionCutoverUnixTime) ||
          (this.localConsumptionPipelineType.equals("local") && timestamp < this.localConsumptionCutoverUnixTime)) {
          partition.put(PREFIX, this.writerPartitionPrefixBackup);
      }
    }
    return partition;
  }
}