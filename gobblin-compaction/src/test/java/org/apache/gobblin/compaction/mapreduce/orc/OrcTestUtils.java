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

package org.apache.gobblin.compaction.mapreduce.orc;

import java.util.Map;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;


public class OrcTestUtils {
  /**
   * Fill in value in OrcStruct with given schema, assuming {@param w} contains the same schema as {@param schema}.
   * {@param schema} is still necessary to given given {@param w} do contains schema information itself, because the
   * actual value type is only available in {@link TypeDescription} but not {@link org.apache.orc.mapred.OrcValue}.
   *
   * For simplicity here are some assumptions:
   * - We only give 3 primitive values and use them to construct compound values. To make it work for different types that
   * can be widened or shrunk to each other, please use value within small range.
   * - For List, Map or Union, make sure there's at least one entry within the record-container.
   * you may want to try createValueRecursively(TypeDescription) instead of {@link OrcStruct#createValue(TypeDescription)}
   */
  public static void fillOrcStructWithFixedValue(WritableComparable w, TypeDescription schema, int unionTag,
      int intValue, String stringValue, boolean booleanValue) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        ((BooleanWritable) w).set(booleanValue);
        break;
      case BYTE:
        ((ByteWritable) w).set((byte) intValue);
        break;
      case SHORT:
        ((ShortWritable) w).set((short) intValue);
        break;
      case INT:
        ((IntWritable) w).set(intValue);
        break;
      case LONG:
        ((LongWritable) w).set(intValue);
        break;
      case FLOAT:
        ((FloatWritable) w).set(intValue * 1.0f);
        break;
      case DOUBLE:
        ((DoubleWritable) w).set(intValue * 1.0);
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        ((Text) w).set(stringValue);
        break;
      case BINARY:
        throw new UnsupportedOperationException("Binary type is not supported in random orc data filler");
      case DECIMAL:
        throw new UnsupportedOperationException("Decimal type is not supported in random orc data filler");
      case DATE:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        throw new UnsupportedOperationException(
            "Timestamp and its derived types is not supported in random orc data filler");
      case LIST:
        OrcList castedList = (OrcList) w;
        // Here it is not trivial to create typed-object in element-type. So this method expect the value container
        // to at least contain one element, or the traversing within the list will be skipped.
        for (Object i : castedList) {
          fillOrcStructWithFixedValue((WritableComparable) i, schema.getChildren().get(0), unionTag, intValue,
              stringValue, booleanValue);
        }
        break;
      case MAP:
        OrcMap castedMap = (OrcMap) w;
        for (Object entry : castedMap.entrySet()) {
          Map.Entry<WritableComparable, WritableComparable> castedEntry =
              (Map.Entry<WritableComparable, WritableComparable>) entry;
          fillOrcStructWithFixedValue(castedEntry.getKey(), schema.getChildren().get(0), unionTag, intValue,
              stringValue, booleanValue);
          fillOrcStructWithFixedValue(castedEntry.getValue(), schema.getChildren().get(1), unionTag, intValue,
              stringValue, booleanValue);
        }
        break;
      case STRUCT:
        OrcStruct castedStruct = (OrcStruct) w;
        int fieldIdx = 0;
        for (TypeDescription child : schema.getChildren()) {
          fillOrcStructWithFixedValue(castedStruct.getFieldValue(fieldIdx), child, unionTag, intValue, stringValue,
              booleanValue);
          fieldIdx += 1;
        }
        break;
      case UNION:
        OrcUnion castedUnion = (OrcUnion) w;
        TypeDescription targetMemberSchema = schema.getChildren().get(unionTag);
        castedUnion.set(unionTag, OrcUtils.createValueRecursively(targetMemberSchema));
        fillOrcStructWithFixedValue((WritableComparable) castedUnion.getObject(), targetMemberSchema, unionTag,
            intValue, stringValue, booleanValue);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + schema.toString());
    }
  }

  /**
   * The simple API: Union tag by default set to 0.
   */
  public static void fillOrcStructWithFixedValue(WritableComparable w, TypeDescription schema, int intValue,
      String stringValue, boolean booleanValue) {
    fillOrcStructWithFixedValue(w, schema, 0, intValue, stringValue, booleanValue);
  }
}
