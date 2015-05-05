package gobblin.publisher;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by akshaynanavati on 5/5/15.
 */
public class KafkaS3Publisher extends BaseS3Publisher {
  public KafkaS3Publisher(State state) {
    super(state);
  }
  @Override
  protected BucketAndKey getBucketAndKey(WorkUnitState task, int branch) {
    String bucket = task.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));
    String key = new SimpleDateFormat("dd/MM/yyyy/").format(new Date()) +
            new Path(task.getProp(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH)).getName();
    return new BucketAndKey(bucket, key);
  }
}
