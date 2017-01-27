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
    EventhubBatch batch = new EventhubBatch(2, 3000);

    JsonObject obj = new JsonObject();
    obj.addProperty("id", 1); // size is 8 bytes

    // Although record is larger than the memory size limit, the first append still succeed
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));

    // The second append should fail
    Assert.assertNull(batch.tryAppend(obj, WriteCallback.EMPTY));
  }

  @Test
  public void testBatch() throws IOException {
    // Assume memory size has only 64 bytes
    EventhubBatch batch = new EventhubBatch(64, 3000);

    JsonObject obj = new JsonObject();
    obj.addProperty("id", 1); // size is 8 bytes

    // Add 8 records into batch
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));

    // Batch has room for 8th record
    Assert.assertEquals(batch.hasRoom(obj), true);
    Assert.assertNotNull(batch.tryAppend(obj, WriteCallback.EMPTY));

    // Batch has no room for 9th record
    Assert.assertEquals(batch.hasRoom(obj), false);
    Assert.assertNull(batch.tryAppend(obj, WriteCallback.EMPTY));
  }
}
