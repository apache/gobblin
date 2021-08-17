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

package org.apache.gobblin.util;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.TextNode;


/**
 * The purpose of this helper class is to make some avro API portable across difference versions of avro.
 * Once all the consumers of Gobblin core library is migrated to Avro 1.9.2, this class can be
 * removed.
 * See also {@link com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper}
 */
public class AvroCompatibilityUtils {
  private static final boolean EARLIER_THAN_AVRO_1_9 = AvroCompatibilityHelper.getRuntimeAvroVersion().earlierThan(
      AvroVersion.AVRO_1_9);
  private static final Constructor<Schema.Field> FIELD_CONSTRUCTOR;
  private static final Method DEFAULT_VALUE_METHOD;

  public static final Object NULL_VALUE = Schema.Field.NULL_DEFAULT_VALUE;

  static {
    if (EARLIER_THAN_AVRO_1_9) {
      try {
        FIELD_CONSTRUCTOR = Schema.Field.class.getConstructor(String.class, Schema.class, String.class, JsonNode.class,
            Schema.Field.Order.class);
        DEFAULT_VALUE_METHOD = Schema.Field.class.getMethod("defaultValue");
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Cannot find Schema.Field method", e);
      }
    } else {
      FIELD_CONSTRUCTOR = null;
      DEFAULT_VALUE_METHOD = null;
    }
  }

  private AvroCompatibilityUtils() {
  }

  /**
   * Compat helper method to replace Schema.Field constructor.
   */
  public static Schema.Field newField(String name, Schema schema, String doc, Object defaultValue, Schema.Field.Order order) {
    if (EARLIER_THAN_AVRO_1_9) {
      JsonNode defaultVal;
      if (defaultValue == null) {
        defaultVal = null;
      } else if (defaultValue == NULL_VALUE) {
        defaultVal = NullNode.getInstance();
      } else if (defaultValue instanceof Integer) {
        defaultVal = new IntNode((Integer) defaultValue);
      } else if (defaultValue instanceof Long) {
        defaultVal = new LongNode((Long) defaultValue);
      } else if (defaultValue instanceof Boolean) {
        defaultVal = BooleanNode.valueOf((Boolean) defaultValue);
      } else if (defaultValue instanceof Float) {
        defaultVal = new DoubleNode((Float) defaultValue);
      } else if (defaultValue instanceof Double) {
        defaultVal = new DoubleNode((Double) defaultValue);
      } else if (defaultValue instanceof String) {
        defaultVal = new TextNode((String) defaultValue);
      } else {
        // Note: this is an incomplete type conversion. But it is probably good enough for galene use case.
        throw new IllegalArgumentException("Unsupported type " + defaultValue.getClass());
      }
      try {
        return FIELD_CONSTRUCTOR.newInstance(name, schema, doc, defaultVal, order);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Failed to create Schema.Field", e);
      }
    } else {
      if (defaultValue == NULL_VALUE) {
        defaultValue = Schema.Field.NULL_VALUE;
      }
      return new Schema.Field(name, schema, doc, defaultValue, order);
    }
  }

  /**
   * Compat helper method to replace Schema.Field constructor.
   */
  public static Schema.Field newField(String name, Schema schema, String doc, Object defaultValue) {
    return newField(name, schema, doc, defaultValue, Schema.Field.Order.ASCENDING);
  }

  /**
   * Compat helper method to replace Schema.Field.defaultVal()
   */
  public static Object fieldDefaultValue(Schema.Field field) {
    if (EARLIER_THAN_AVRO_1_9) {
      JsonNode defaultValue;
      try {
        defaultValue = (JsonNode) DEFAULT_VALUE_METHOD.invoke(field);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Failed to get defaultValue for field", e);
      }
      if (defaultValue == null) {
        return null;
      } else if (defaultValue.isNull()) {
        return NULL_VALUE;
      } else if (defaultValue.isBoolean()) {
        return defaultValue.asBoolean();
      } else if (defaultValue.isInt()) {
        return defaultValue.asInt();
      } else if (defaultValue.isLong()) {
        return defaultValue.asLong();
      } else if (defaultValue.isDouble()) {
        return defaultValue.asDouble();
      } else if (defaultValue.isTextual()) {
        return defaultValue.asText();
      } else {
        // Note: this is an incomplete type conversion. But it is probably good enough for galene use case.
        throw new RuntimeException("Unsupported value conversion: " + defaultValue);
      }
    } else {
      Object defaultValue = field.defaultVal();
      return (defaultValue == NULL_VALUE) ? NULL_VALUE : defaultValue;
    }
  }
}
