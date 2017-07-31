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

package org.apache.gobblin.fork;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link CopyableSchema}.
 *
 * @author Yinan Li
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
