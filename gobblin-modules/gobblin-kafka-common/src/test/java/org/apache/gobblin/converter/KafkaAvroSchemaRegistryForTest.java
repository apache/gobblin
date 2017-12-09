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

package org.apache.gobblin.converter;

import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistryFactory;
import java.util.Properties;
import org.apache.avro.Schema;

/**
 * Override some methods of {@link KafkaAvroSchemaRegistry} for use in {@link EnvelopeSchemaConverterTest}
 *
 * @deprecated Checkout {@link EnvelopePayloadExtractingConverterTest} for how to mock a {@link KafkaSchemaRegistry}
 */
@Deprecated
public class KafkaAvroSchemaRegistryForTest extends KafkaAvroSchemaRegistry {
  public static class Factory implements KafkaSchemaRegistryFactory {
    public Factory() {}

    public KafkaSchemaRegistry create(Properties props) {
      return new KafkaAvroSchemaRegistryForTest(props);
    }
  }

  public KafkaAvroSchemaRegistryForTest(Properties props) {
    super(props);
  }

  @Override
  public Schema getSchemaByKey(String key) {
    if (key.equals(EnvelopeSchemaConverterTest.SCHEMA_KEY)) {
      return EnvelopeSchemaConverterTest.mockSchema;
    } else {
      return null;
    }
  }
}