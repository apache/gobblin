package gobblin.eventhub.writer;

import java.io.IOException;

import gobblin.writer.BytesBoundedBatch;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.writer.WriteCallback;


public class EventhubBatchTest {

  @Test
  public void testBatchWithLargeRecord() throws IOException {
    // Assume memory size has only 2 bytes
    BytesBoundedBatch batch = new BytesBoundedBatch(8, 3000);

    String record = "abcdefgh";

    // Record is larger than the memory size limit, the first append should fail
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY));

    // The second append should still fail
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY));
  }

  @Test
  public void testBatch() throws IOException {
    // Assume memory size has only 200 bytes
    BytesBoundedBatch batch = new BytesBoundedBatch(200, 3000);

    // Add additional 15 bytes overhead, total size is 27 bytes
    String record = "abcdefgh";

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
