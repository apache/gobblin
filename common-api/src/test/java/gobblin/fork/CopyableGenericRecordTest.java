package gobblin.fork;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link CopyableGenericRecord}.
 *
 * @author ynli
 */
@Test(groups = "gobblin.fork")
public class CopyableGenericRecordTest {

  // Test Avro schema
  private static final String AVRO_SCHEMA = "{\"namespace\": \"example.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"User\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"name\", \"type\": \"string\"},\n" +
      "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
      "     {\"name\": \"favorite_colors\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n" +
      " ]\n" +
      "}";

  @Test
  public void testCopy() throws CopyNotSupportedException {
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(AVRO_SCHEMA));
    record.put("name", "foo");
    record.put("favorite_number", 68);
    record.put("favorite_colors", Arrays.asList("blue", "black", "red"));
    CopyableGenericRecord copyableGenericRecord = new CopyableGenericRecord(record);
    GenericRecord copy = copyableGenericRecord.copy();
    Assert.assertEquals(record, copy);
    copy.put("name", "bar");
    Assert.assertNotEquals(record, copy);

  }
}
