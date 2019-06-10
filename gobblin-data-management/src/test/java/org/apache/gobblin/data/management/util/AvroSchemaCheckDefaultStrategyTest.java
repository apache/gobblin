/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.data.management.util;

import java.io.File;
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

    //test complex schema
    toValidate = new Schema.Parser().parse(new File(AvroSchemaCheckDefaultStrategy.class.getClassLoader().getResource("avroSchemaCheckStrategyTest/toValidateSchema.avsc").getFile()));
    expected = new Schema.Parser().parse(new File(AvroSchemaCheckDefaultStrategy.class.getClassLoader().getResource("avroSchemaCheckStrategyTest/expectedSchema.avsc").getFile()));
    Assert.assertTrue(strategy.compare(expected, toValidate));
  }
}
