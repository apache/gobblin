package gobblin.fork;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link CopyableSchema}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.fork"})
public class CopyableSchemaTest {

  // Test Avro schema
  private static final String AVRO_SCHEMA = "{\"namespace\": \"example.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"User\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"name\", \"type\": \"string\"},\n" +
      "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
      "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
      " ]\n" +
      "}";

  @Test
  public void testCopy() throws CopyNotSupportedException {
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    CopyableSchema copyableSchema = new CopyableSchema(schema);
    Schema copy = copyableSchema.copy();
    Assert.assertEquals(schema, copy);
    copy.addProp("foo", "bar");
    Assert.assertNotEquals(schema, copy);
  }
}
