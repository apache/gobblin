package gobblin.eventhub.writer;

import gobblin.writer.SequentialAndTTLBasedBatchAccumulator;

import java.util.Properties;

/**
 * Created by kuyu on 4/21/17.
 */
public class EventhubBatchAccumulator extends SequentialAndTTLBasedBatchAccumulator<String> {
  public EventhubBatchAccumulator (Properties properties) {
    super(properties);
  }

  public EventhubBatchAccumulator (long batchSizeLimit, long expireInMilliSecond, long capacity) {
    super (batchSizeLimit, expireInMilliSecond, capacity);
  }
}
