package org.apache.gobblin.writer.partitioner;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
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