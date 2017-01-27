package gobblin.eventhub.writer;

import java.io.IOException;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.gson.JsonObject;

import gobblin.writer.Batch;
import gobblin.writer.WriteCallback;


public class EventhubAccumulatorTest {
  @Test
  public void testAccumulator() throws IOException, InterruptedException{

    EventhubBatchAccumulator accumulator = new EventhubBatchAccumulator(128 * 1024, 3000);
    JsonObject obj = new JsonObject();
    obj.addProperty("id", 1); // size is 8 bytes
    long unit = obj.toString().getBytes(Charsets.UTF_8).length;

    // Assuming batch size is 128K, and each record has 8 bytes
    // Adding 16K records should not overflow the memory of first batch
    // The first batch is still waiting for more incoming records so it is not ready to be sent out
    long bytes = accumulator.getMemSizeLimit();
    for (int i=0; i<bytes/unit; ++i) {
      accumulator.append(obj, WriteCallback.EMPTY);
    }

    Iterator<Batch<JsonObject>> iterator = accumulator.iterator();
    Assert.assertEquals(iterator.hasNext(), false);

    // Now add another record, which should result in the overflow of first batch
    // This record should be added into the second batch, now the first batch should be available
    accumulator.append(obj, WriteCallback.EMPTY);
    Assert.assertEquals(iterator.hasNext(), true);

    // Remove the first batch, the second batch should now contains one record and it should
    // not be ready to pop out because 1) size is not exceed the limit 2) TTL is not expired
    iterator.next();
    Assert.assertEquals(iterator.hasNext(), false);

    Thread.sleep(accumulator.getExpireInMilliSecond());

    // Now the TTL should be expired, the second batch should be available
    Assert.assertEquals(iterator.hasNext(), true);
    Batch<JsonObject> batch = iterator.next();
    Assert.assertEquals(batch.getRecords().size(), 1);
  }
}
