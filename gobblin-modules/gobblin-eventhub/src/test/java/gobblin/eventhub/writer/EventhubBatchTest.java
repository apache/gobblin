package gobblin.eventhub.writer;

import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.writer.WriteCallback;


public class EventhubBatchTest {

  @Test
  public void testBatchWithLargeRecord() throws IOException {
    // Assume memory size has only 2 bytes
    EventhubBatch batch = new EventhubBatch(8, 3000);

    byte[] record = new byte[8];

    // Record is larger than the memory size limit, the first append should fail
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY));

    // The second append should still fail
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY));
  }

  @Test
  public void testBatch() throws IOException {
    // Assume memory size has only 200 bytes
    EventhubBatch batch = new EventhubBatch(200, 3000);

    // single record has 8 bytes, it converts to 12 bytes due to Base64 encoding
    // Add additional 15 bytes overhead, total size is 27 bytes
    byte[] record = new byte[8];

    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));

    // Batch has room for 7th record
    Assert.assertEquals(batch.hasRoom(record), true);
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY));

    // Batch has no room for 8th record
    Assert.assertEquals(batch.hasRoom(record), false);
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY));
  }
}
