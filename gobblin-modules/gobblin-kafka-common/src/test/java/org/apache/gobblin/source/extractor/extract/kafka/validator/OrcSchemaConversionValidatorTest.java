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
package org.apache.gobblin.source.extractor.extract.kafka.validator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistry;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.kafka.schemareg.SchemaRegistryException;
import org.apache.gobblin.kafka.serialize.MD5Digest;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrcSchemaConversionValidatorTest {
  @Test
  public void testOrcSchemaConversionValidator() throws Exception {
    // topic 1's schema has max depth = 1
    KafkaTopic topic1 = new KafkaTopic("topic1", ImmutableList.of());
    // topic 2's schema has max depth = 2
    KafkaTopic topic2 = new KafkaTopic("topic2", ImmutableList.of());
    // topic 3's schema has recursive filed reference
    KafkaTopic topic3 = new KafkaTopic("topic3", ImmutableList.of());

    State state = new State();
    // Use the test schema registry to get the schema for the above topic.
    state.setProp(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS, TestKafkaSchemaRegistry.class.getName());

    // Validate with default max_recursive_depth (=200).
    OrcSchemaConversionValidator validator = new OrcSchemaConversionValidator(state);
    Assert.assertTrue(validator.validate(topic1));  // Pass validation
    Assert.assertTrue(validator.validate(topic2));  // Pass validation
    Assert.assertFalse(validator.validate(topic3)); // Fail validation, default max_recursive_depth = 200, the validation returns early

    // Validate with max_recursive_depth=1
    state.setProp(OrcSchemaConversionValidator.MAX_RECURSIVE_DEPTH_KEY, 1);
    Assert.assertTrue(validator.validate(topic1));  // Pass validation
    Assert.assertFalse(validator.validate(topic2)); // Fail validation, because max_recursive_depth is set to 1,  the validation returns early
    Assert.assertFalse(validator.validate(topic3)); // Fail validation, because max_recursive_depth is set to 1, the validation returns early
  }

  @Test
  public void testGetLatestSchemaFail() throws Exception {
    KafkaTopic topic1 = new KafkaTopic("topic1", ImmutableList.of());
    KafkaTopic topic2 = new KafkaTopic("topic2", ImmutableList.of());
    KafkaTopic topic3 = new KafkaTopic("topic3", ImmutableList.of());
    State state = new State();
    state.setProp(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS, BadKafkaSchemaRegistry.class.getName());

    OrcSchemaConversionValidator validator = new OrcSchemaConversionValidator(state);
    // Validator should always return PASS when it fails to get latest schema.
    Assert.assertTrue(validator.validate(topic1));
    Assert.assertTrue(validator.validate(topic2));
    Assert.assertTrue(validator.validate(topic3));
  }

  // A KafkaSchemaRegistry class that returns the hardcoded schemas for the test topics.
  public static class TestKafkaSchemaRegistry implements KafkaSchemaRegistry<MD5Digest, Schema> {
    private final String schemaMaxInnerFieldDepthIs1 = "{"
        + "\"type\": \"record\","
        + "  \"name\": \"test\","
        + "  \"fields\": ["
        + "    {\n"
        + "      \"name\": \"id\","
        + "      \"type\": \"int\""
        + "    },"
        + "    {"
        + "      \"name\": \"timestamp\","
        + "      \"type\": \"string\""
        + "    }"
        + "  ]"
        + "}";

    private final String schemaMaxInnerFieldDepthIs2 = "{"
        + "  \"type\": \"record\","
        + "  \"name\": \"nested\","
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"nestedId\","
        + "      \"type\": {\n"
        + "        \"type\": \"array\","
        + "        \"items\": \"string\""
        + "      }"
        + "    },"
        + "    {"
        + "      \"name\": \"timestamp\","
        + "      \"type\": \"string\""
        + "    }"
        + "  ]"
        + "}";

    private final String schemaWithRecursiveRef = "{"
        + "  \"type\": \"record\","
        + "  \"name\": \"TreeNode\","
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"value\","
        + "      \"type\": \"int\""
        + "    },"
        + "    {"
        + "      \"name\": \"children\","
        + "      \"type\": {"
        + "        \"type\": \"array\","
        + "        \"items\": \"TreeNode\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";
    private final Map<String, Schema> topicToSchema;

    public TestKafkaSchemaRegistry(Properties props) {
      topicToSchema = ImmutableMap.of(
          "topic1", new Schema.Parser().parse(schemaMaxInnerFieldDepthIs1),
          "topic2", new Schema.Parser().parse(schemaMaxInnerFieldDepthIs2),
          "topic3", new Schema.Parser().parse(schemaWithRecursiveRef));
    }
    @Override
    public Schema getLatestSchema(String topicName) {
      return topicToSchema.get(topicName);
    }

    @Override
    public MD5Digest register(String name, Schema schema) {
      return null;
    }

    @Override
    public Schema getById(MD5Digest id) {
      return null;
    }

    @Override
    public boolean hasInternalCache() {
      return false;
    }
  }

  // A KafkaSchemaRegistry class that always fail to get latest schema.
  public static class BadKafkaSchemaRegistry implements KafkaSchemaRegistry<MD5Digest, Schema> {
    public BadKafkaSchemaRegistry(Properties props) {
    }

    @Override
    public Schema getLatestSchema(String name) throws IOException, SchemaRegistryException {
      throw new SchemaRegistryException("Exception in getLatestSchema()");
    }

    @Override
    public MD5Digest register(String name, Schema schema) throws IOException, SchemaRegistryException {
      return null;
    }

    @Override
    public Schema getById(MD5Digest id) throws IOException, SchemaRegistryException {
      return null;
    }

    @Override
    public boolean hasInternalCache() {
      return false;
    }
  }
}
