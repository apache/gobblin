package org.apache.gobblin.writer.partitioner;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.joda.time.DateTime;


@Slf4j
public class CustomPartitionerForMigration extends TimeBasedAvroWriterPartitioner {
  public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";
  public long LOCAL_CONSUMPTION_CUTOVER_UNIX_TIME;
  public boolean LOCAL_CONSUMPTION_PROGRESS;
  public String PIPELINE_TYPE;

  public CustomPartitionerForMigration(State state) {
    this(state, 1, 0);
  }

  public CustomPartitionerForMigration(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.LOCAL_CONSUMPTION_CUTOVER_UNIX_TIME = state.getPropAsLong(ConfigurationKeys.LOCAL_CONSUMPTION_CUTOVER_UNIX);
    this.LOCAL_CONSUMPTION_PROGRESS = state.getPropAsBoolean(ConfigurationKeys.LOCAL_CONSUMPTION_ON);
    this.PIPELINE_TYPE = state.getProp(ConfigurationKeys.LOCAL_CONSUMPTION_PIPELINE_TYPE);
  }

  @Override
  protected String getWriterPartitionPrefix(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_PREFIX, numBranches, branchId);
    if (state.getPropAsBoolean(ConfigurationKeys.LOCAL_CONSUMPTION_ON)) {
      propName = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.LOCAL_CONSUMPTION_WRITER_PARTITION_PREFIX, numBranches, branchId);
    }
    return state.getProp(propName, StringUtils.EMPTY);
  }

  @Override
  public GenericRecord partitionForRecord(GenericRecord record) {
    long timestamp = timeUnit.toMillis(getRecordTimestamp(record));
    GenericRecord partition = new GenericData.Record(this.schema);
    if (!Strings.isNullOrEmpty(this.writerPartitionPrefix)) {
      partition.put(PREFIX, this.writerPartitionPrefix);
    }
    if (!Strings.isNullOrEmpty(this.writerPartitionSuffix)) {
      partition.put(SUFFIX, this.writerPartitionSuffix);
    }

    if (this.timestampToPathFormatter.isPresent()) {
      String partitionedPath = getPartitionedPath(timestamp);
      partition.put(PARTITIONED_PATH, partitionedPath);
    } else {
      DateTime dateTime = new DateTime(timestamp, this.timeZone);
      partition.put(this.granularity.toString(), this.granularity.getField(dateTime));
    }

    if (this.LOCAL_CONSUMPTION_PROGRESS) {
      if (this.PIPELINE_TYPE.equals("aggregate")) {
        // Drop the prefix if the timestamp is lower than the cutover unix timestamp
        if (timestamp < this.LOCAL_CONSUMPTION_CUTOVER_UNIX_TIME) {
          partition.put(PREFIX, "");
        }
      }
      else if (this.PIPELINE_TYPE.equals("local")) {
        if (timestamp >= this.LOCAL_CONSUMPTION_CUTOVER_UNIX_TIME) {
          partition.put(PREFIX, "");
        }
      }
    }

    return partition;
  }
}
