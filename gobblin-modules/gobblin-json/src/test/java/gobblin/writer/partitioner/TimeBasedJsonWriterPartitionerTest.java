package gobblin.writer.partitioner;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import gobblin.configuration.State;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by tamasnemeth on 08/02/17.
 */
public class TimeBasedJsonWriterPartitionerTest {

  @Test
  public void testGetRecordTimestampWithUnixTimestamp() throws Exception {
    State state = createState("timestamp", true);
    TimeBasedJsonWriterPartitioner partitioner = new TimeBasedJsonWriterPartitioner(state);

    JsonElement json = new JsonParser().parse("{timestamp: 123456}");

    long timestamp = partitioner.getRecordTimestamp(json);

    Assert.assertEquals(123456000, timestamp);

  }

  @Test
  public void testGetRecordTimestampWithJavaTimestamp() throws Exception {
    State state = createState("timestamp", false);
    TimeBasedJsonWriterPartitioner partitioner = new TimeBasedJsonWriterPartitioner(state);

    JsonElement json = new JsonParser().parse("{timestamp: 123456}");

    long timestamp = partitioner.getRecordTimestamp(json);

    Assert.assertEquals(123456, timestamp);

  }

  @Test
  public void testGetRecordTimestampWithMultipleColumnSpecified() throws Exception {
    State state = createState("nonexistent1, nonexistent2, timestamp", false);
    TimeBasedJsonWriterPartitioner partitioner = new TimeBasedJsonWriterPartitioner(state);

    JsonElement json = new JsonParser().parse("{timestamp: 123456}");

    long timestamp = partitioner.getRecordTimestamp(json);

    Assert.assertEquals(123456, timestamp);

  }

  @Test
  public void testGetRecordTimestampWithNonExistentColumnSpecified() throws Exception {
    State state = createState("nonexistent1, nonexistent2", false);
    TimeBasedJsonWriterPartitioner partitioner = new TimeBasedJsonWriterPartitioner(state);

    JsonElement json = new JsonParser().parse("{timestamp: 123456}");
    long now = System.currentTimeMillis();

    long timestamp = partitioner.getRecordTimestamp(json);

    Assert.assertTrue(timestamp >= now);

  }

  private State createState(String partitionColumns, boolean isUnixTimestamp) {
    State state = new State();
    state.setProp(TimeBasedJsonWriterPartitioner.WRITER_PARTITION_COLUMNS, partitionColumns);
    state.setProp(TimeBasedJsonWriterPartitioner.WRITER_TIMESTAMP_IN_UNIX, isUnixTimestamp);

    return state;
  }

}