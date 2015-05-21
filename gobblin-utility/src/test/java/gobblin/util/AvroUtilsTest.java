package gobblin.util;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.testng.annotations.Test;


public class AvroUtilsTest {

  private static final String AVRO_DIR = "gobblin-utility/src/test/resources/avroDirParent/";

  @Test
  public void testGetDirectorySchema() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    Path mockAvroFilePath = new Path(AVRO_DIR);
    Assert.assertNotNull(AvroUtils.getDirectorySchema(mockAvroFilePath, conf, true));
  }

  /**
   * Test nullifying fields for non-union types, including array.
   */
  @Test
  public void testNullifyFieldForNonUnionSchemaMerge() {
    Schema oldSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, " + "{\"name\": \"number\", \"type\": \"int\"}" + "]}");

    Schema newSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, " + "{\"name\": \"number\", \"type\": [\"null\", \"int\"]}"
            + "]}");

    Assert.assertEquals(expectedOutputSchema1, AvroUtils.nullifyFiledsForSchemaMerge(oldSchema1, newSchema1));

    Schema oldSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]}");

    Schema newSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}" + "]}");

    Assert.assertEquals(expectedOutputSchema2, AvroUtils.nullifyFiledsForSchemaMerge(oldSchema2, newSchema2));
  }

  /**
   * Test nullifying fields for union type. One case does not have "null", and the other case already has a default "null".
   */
  @Test
  public void testNullifyFieldForUnionSchemaMerge() {

    Schema oldSchema1 =
        new Schema.Parser()
            .parse("{\"type\":\"record\", \"name\":\"test\", "
                + "\"fields\":["
                + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"number\", \"type\": [{\"type\": \"string\"}, {\"type\": \"array\", \"items\": \"string\"}]}"
                + "]}");

    Schema newSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema1 =
        new Schema.Parser()
            .parse("{\"type\":\"record\", \"name\":\"test\", "
                + "\"fields\":["
                + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"string\"}, {\"type\": \"array\", \"items\": \"string\"}]}"
                + "]}");

    Assert.assertEquals(expectedOutputSchema1, AvroUtils.nullifyFiledsForSchemaMerge(oldSchema1, newSchema1));

    Schema oldSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}" + "]}");

    Schema newSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}" + "]}");

    Assert.assertEquals(expectedOutputSchema2, AvroUtils.nullifyFiledsForSchemaMerge(oldSchema2, newSchema2));
  }
}
