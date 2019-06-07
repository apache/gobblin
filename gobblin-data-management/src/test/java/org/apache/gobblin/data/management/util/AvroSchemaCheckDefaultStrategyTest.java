package org.apache.gobblin.data.management.util;

import org.apache.avro.Schema;
import org.apache.gobblin.util.schema_check.AvroSchemaCheckDefaultStrategy;
import org.junit.Assert;
import org.testng.annotations.Test;


public class AvroSchemaCheckDefaultStrategyTest {
  @Test
  public void testSchemCheckStrategy() throws Exception {
    //test when it's compatible
    Schema toValidate = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
    Schema expected = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo\",\"type\":[\"null\",\"long\"],\"doc\":\"this is for test\",\"default\":null}]}");
    AvroSchemaCheckDefaultStrategy strategy = new AvroSchemaCheckDefaultStrategy();
    org.junit.Assert.assertTrue(strategy.compare(expected, toValidate));

    //test when field name is different
    expected = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo1\",\"type\":[\"null\",\"long\"],\"doc\":\"this is for test\",\"default\":null}]}");
    org.junit.Assert.assertFalse(strategy.compare(expected, toValidate));

    //test when the type change
    expected = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo\",\"type\":[\"null\",\"int\"],\"doc\":\"this is for test\",\"default\":null}]}");
    org.junit.Assert.assertFalse(strategy.compare(expected, toValidate));

    expected = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo\",\"type\":[\"null\",\"float\"],\"doc\":\"this is for test\",\"default\":null}]}");
    Assert.assertFalse(strategy.compare(expected, toValidate));
  }
}
