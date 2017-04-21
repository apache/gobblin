package gobblin.eventhub.writer;

import gobblin.writer.SequentialAndTTLBasedBatchAccumulator;

import java.util.Properties;

/**
 * Simply a ttl based batch accumulator for eventhub with string type
 */
public class EventhubBatchAccumulator extends SequentialAndTTLBasedBatchAccumulator<String> {
  public EventhubBatchAccumulator (Properties properties) {
    super(properties);
  }

  public EventhubBatchAccumulator (long batchSizeLimit, long expireInMilliSecond, long capacity) {
    super (batchSizeLimit, expireInMilliSecond, capacity);
  }
}
