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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.kafka.serialize.GsonDeserializerBase;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * A {@link KafkaSource} that reads kafka record as {@link JsonObject}
 */
public class Kafka09JsonSource extends KafkaSource<JsonArray, JsonObject> {
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    if (!state.contains(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY)) {
      state.setProp(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY,
          KafkaGsonDeserializer.class.getName());
    }
    return super.getWorkunits(state);
  }

  @Override
  public Extractor<JsonArray, JsonObject> getExtractor(WorkUnitState state)
      throws IOException {
    return new JsonExtractor(state);
  }

  static final class JsonExtractor extends KafkaExtractor<JsonArray, JsonObject> {
    private static final String JSON_SCHEMA = "source.kafka.json.schema";
    private static final JsonParser JSON_PARSER = new JsonParser();
    private final JsonArray schema;

    JsonExtractor(WorkUnitState state) {
      super(state);
      String schemaStr = state.getProp(JSON_SCHEMA);
      if (StringUtils.isEmpty(schemaStr)) {
        throw new RuntimeException("Missing configuration: " + JSON_SCHEMA);
      }
      this.schema = JSON_PARSER.parse(schemaStr).getAsJsonArray();
    }

    @Override
    public JsonArray getSchema()
        throws IOException {
      return schema;
    }

    @Override
    protected JsonObject decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord)
        throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A specific kafka {@link Deserializer} that deserializes record as JasonObject
   */
  public static final class KafkaGsonDeserializer extends GsonDeserializerBase<JsonObject> implements Deserializer<JsonObject> {
  }
}
