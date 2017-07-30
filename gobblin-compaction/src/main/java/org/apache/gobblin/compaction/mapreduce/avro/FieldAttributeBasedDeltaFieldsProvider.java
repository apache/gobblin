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

package gobblin.compaction.mapreduce.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import lombok.extern.slf4j.Slf4j;


/**
 * Extends {@link AvroDeltaFieldNameProvider}, which relies on field {@link #ATTRIBUTE_FIELD} in record schema to get the delta schema.
 */
@Slf4j
public class FieldAttributeBasedDeltaFieldsProvider implements AvroDeltaFieldNameProvider {
  public static final String ATTRIBUTE_FIELD =
      "gobblin.compaction." + FieldAttributeBasedDeltaFieldsProvider.class.getSimpleName() + ".deltaAttributeField";
  public static final String DELTA_PROP_NAME =
      "gobblin.compaction." + FieldAttributeBasedDeltaFieldsProvider.class.getSimpleName() + ".deltaPropName";
  public static final String DEFAULT_DELTA_PROP_NAME = "delta";
  private final String attributeField;
  private final String deltaPropName;
  private final LoadingCache<Schema, List<String>> recordSchemaToDeltaSchemaCache;

  public FieldAttributeBasedDeltaFieldsProvider (Configuration conf) {
    this.attributeField = conf.get(ATTRIBUTE_FIELD);
    Preconditions.checkArgument(attributeField != null, "Missing config " + ATTRIBUTE_FIELD);
    this.deltaPropName = conf.get(DELTA_PROP_NAME, DEFAULT_DELTA_PROP_NAME);
    this.recordSchemaToDeltaSchemaCache=
        CacheBuilder.newBuilder().maximumSize(100).build(new CacheLoader<Schema, List<String>>() {
          @Override
          public List<String> load(Schema schema)
              throws Exception {
            return getDeltaFieldNamesForNewSchema(schema);
          }
        });
  }

  @Override
  public List<String> getDeltaFieldNames(GenericRecord record) {
    try {
      return recordSchemaToDeltaSchemaCache.get(record.getSchema());
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getDeltaFieldNamesForNewSchema(Schema originalSchema) {
    List<String> deltaFields = new ArrayList<>();
    for (Field field : originalSchema.getFields()) {
      String deltaAttributeField = field.getJsonProp(this.attributeField).getValueAsText();
      ObjectNode objectNode = getDeltaPropValue(deltaAttributeField);
      if (objectNode == null || objectNode.get(this.deltaPropName) == null) {
        continue;
      }
      if (Boolean.parseBoolean(objectNode.get(this.deltaPropName).toString())) {
        deltaFields.add(field.name());
      }
    }
    log.info("Will use delta fields: " + deltaFields);
    return deltaFields;
  }

  private ObjectNode getDeltaPropValue(String json) {
    try {
      JsonFactory jf = new JsonFactory();
      JsonParser jp = jf.createJsonParser(json);
      ObjectMapper objMap = new ObjectMapper(jf);
      jp.setCodec(objMap);
      return (ObjectNode) jp.readValueAsTree();
    } catch (IOException e) {
      return null;
    }
  }
}
