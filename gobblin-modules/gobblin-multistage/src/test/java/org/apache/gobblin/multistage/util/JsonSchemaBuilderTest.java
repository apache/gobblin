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

package org.apache.gobblin.multistage.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonSchemaBuilder;
import org.junit.Assert;
import org.testng.annotations.Test;


@Test
public class JsonSchemaBuilderTest {
  @Test
  public void testReverseAvroSchema() {
    // Test parsing an avro schema to a JsonSchemaBuilder
    JsonObject avro = new Gson().fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/json/nested-avro-schema.json")), JsonObject.class);
    JsonSchemaBuilder builder = JsonSchemaBuilder.fromAvro(avro);

    // Test build into a Avro style schema that can be accepted by Json2Avro converter
    JsonArray outputSchema = builder.buildAltSchema().getAsJsonArray();
    Assert.assertEquals(outputSchema.size(), 8);

    // Test build into JsonSchema style schema, this is for backward compatibility
    JsonSchema jsonSchema = builder.buildJsonSchema();
    Assert.assertEquals(jsonSchema.getSchema().get("items").getAsJsonObject().entrySet().size(), 8);
  }
}
