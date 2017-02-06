package gobblin.eventhub.writer;

import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonObject;
import gobblin.writer.WriteCallback;


public class EventhubBatchTest {

  @Test
  public void testBatchWithLargeRecord() throws IOException {
    // Assume memory size has only 2 bytes
    EventhubBatch batch = new EventhubBatch(8, 3000);

    JsonObject obj = new JsonObject();
    obj.addProperty("id", 1); // size is 8 bytes

    // Record is larger than the memory size limit, the first append should fail
    Assert.assertNull(batch.tryAppend(obj, WriteCallback.EMPTY));

    // The second append should still fail
    Assert.assertNull(batch.tryAppend(obj, WriteCallback.EMPTY));
  }

  @Test
  public void testBatch() throws IOException {
    // Assume memory size has only 200 bytes
    EventhubBatch batch = new EventhubBatch(200, 3000);

    JsonObject record = new JsonObject();

    // single record has 8 bytes, but overhead has 15 bytes, total size is 23 bytes
    record.addProperty("id", 1);

    // Add 8 records into batch
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));

    // Batch has room for 8th record
    Assert.assertEquals(batch.hasRoom(record), true);
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));

    // Batch has no room for 9th record
    Assert.assertEquals(batch.hasRoom(record), false);
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY));
  }
}
