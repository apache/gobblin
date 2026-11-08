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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistry;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryFactory;
import org.apache.gobblin.kafka.schemareg.SchemaRegistryException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.orc.AvroOrcSchemaConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OrcSchemaConversionValidator extends TopicValidatorBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrcSchemaConversionValidator.class);

  public static final String MAX_RECURSIVE_DEPTH_KEY = "gobblin.kafka.topicValidators.orcSchemaConversionValidator.maxRecursiveDepth";
  public static final int DEFAULT_MAX_RECURSIVE_DEPTH = 200;

  private final KafkaSchemaRegistry schemaRegistry;

  public OrcSchemaConversionValidator(State sourceState) {
    super(sourceState);
    this.schemaRegistry = KafkaSchemaRegistryFactory.getSchemaRegistry(sourceState.getProperties());
  }

  @Override
  public boolean validate(KafkaTopic topic) throws Exception {
    LOGGER.debug("Validating ORC schema conversion for topic {}", topic.getName());
    try {
      Schema schema = (Schema) this.schemaRegistry.getLatestSchema(topic.getName());
      // Try converting the avro schema to orc schema to check if any errors.
      int maxRecursiveDepth = this.state.getPropAsInt(MAX_RECURSIVE_DEPTH_KEY, DEFAULT_MAX_RECURSIVE_DEPTH);
      AvroOrcSchemaConverter.tryGetOrcSchema(schema, 0, maxRecursiveDepth);
    } catch (StackOverflowError e) {
      LOGGER.warn("Failed to covert latest schema to ORC schema for topic: {}", topic.getName());
      return false;
    } catch (IOException | SchemaRegistryException e) {
      LOGGER.warn("Failed to get latest schema for topic: {}, validation is skipped, exception: ", topic.getName(), e);
      return true;
    }
    return true;
  }
}
