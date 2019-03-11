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

import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrcKeyComparatorTest {
  @Test
  public void testSimpleComparator() throws Exception {
    OrcKeyComparator comparator = new OrcKeyComparator();
    Configuration conf = new Configuration();
    String orcSchema = "struct<i:int,j:int>";
    TypeDescription schema = TypeDescription.fromString(orcSchema);
    conf.set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), orcSchema);
    Assert.assertEquals(conf.get(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()), orcSchema);
    comparator.setConf(conf);

    OrcStruct record0 = (OrcStruct) OrcStruct.createValue(schema);
    record0.setFieldValue("i", new IntWritable(1));
    record0.setFieldValue("j", new IntWritable(2));

    OrcStruct record1 = (OrcStruct) OrcStruct.createValue(schema);
    record1.setFieldValue("i", new IntWritable(3));
    record1.setFieldValue("j", new IntWritable(4));

    OrcStruct record2 = (OrcStruct) OrcStruct.createValue(schema);
    record2.setFieldValue("i", new IntWritable(3));
    record2.setFieldValue("j", new IntWritable(4));

    OrcKey orcKey0 = new OrcKey();
    orcKey0.key = record0;
    OrcKey orcKey1 = new OrcKey();
    orcKey1.key = record1;
    OrcKey orcKey2 = new OrcKey();
    orcKey2.key = record2;

    Assert.assertTrue(comparator.compare(orcKey0, orcKey1) < 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey2) == 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey0) > 0);
  }

  @Test
  public void testComplextRecord_array() throws Exception {
    OrcKeyComparator comparator = new OrcKeyComparator();
    Configuration conf = new Configuration();

    TypeDescription listSchema = TypeDescription.createList(TypeDescription.createString());
    TypeDescription schema = TypeDescription.createStruct()
        .addField("a", TypeDescription.createInt())
        .addField("b", listSchema);

    conf.set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), schema.toString());
    Assert.assertEquals(conf.get(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()), schema.toString());
    comparator.setConf(conf);

    // base record
    OrcStruct record0 = (OrcStruct) OrcStruct.createValue(schema);
    record0.setFieldValue("a", new IntWritable(1));
    OrcList orcList0 = createOrcList(3, listSchema, 3);
    record0.setFieldValue("b", orcList0);

    // the same as base but different object, expecting equal to each other.
    OrcStruct record1 = (OrcStruct) OrcStruct.createValue(schema);
    record1.setFieldValue("a", new IntWritable(1));
    OrcList orcList1 = createOrcList(3, listSchema, 3);
    record1.setFieldValue("b", orcList1);

    // Diff in int field
    OrcStruct record2 = (OrcStruct) OrcStruct.createValue(schema);
    record2.setFieldValue("a", new IntWritable(2));
    OrcList orcList2 = createOrcList(3, listSchema, 3);
    record2.setFieldValue("b", orcList2);

    // Diff in array field: 1
    OrcStruct record3 = (OrcStruct) OrcStruct.createValue(schema);
    record3.setFieldValue("a", new IntWritable(1));
    OrcList orcList3 = createOrcList(3, listSchema, 5);
    record3.setFieldValue("b", orcList3);

    // Diff in array field: 2
    OrcStruct record4 = (OrcStruct) OrcStruct.createValue(schema);
    record4.setFieldValue("a", new IntWritable(1));
    OrcList orcList4 = createOrcList(4, listSchema, 3);
    record4.setFieldValue("b", orcList4);


    OrcKey orcKey0 = new OrcKey();
    orcKey0.key = record0;
    OrcKey orcKey1 = new OrcKey();
    orcKey1.key = record1;
    OrcKey orcKey2 = new OrcKey();
    orcKey2.key = record2;
    OrcKey orcKey3 = new OrcKey();
    orcKey3.key = record3;
    OrcKey orcKey4 = new OrcKey();
    orcKey4.key = record4;

    Assert.assertTrue(comparator.compare(orcKey0, orcKey1) == 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey2) < 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey3) < 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey4) < 0);
  }

  @Test
  public void testComplexRecord_map() throws Exception {
    OrcKeyComparator comparator = new OrcKeyComparator();
    Configuration conf = new Configuration();
    TypeDescription mapFieldSchema =
        TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createString());
    TypeDescription schema =
        TypeDescription.createStruct().addField("a", TypeDescription.createInt()).addField("b", mapFieldSchema);

    conf.set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), schema.toString());
    Assert.assertEquals(conf.get(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()), schema.toString());
    comparator.setConf(conf);

    // base record
    OrcStruct record0 = (OrcStruct) OrcStruct.createValue(schema);
    record0.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap = createOrcMap(new Text("key"), new Text("value"), mapFieldSchema);
    record0.setFieldValue("b", orcMap);

    // key value both differ
    OrcStruct record1 = (OrcStruct) OrcStruct.createValue(schema);
    record1.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap1 = createOrcMap(new Text("key_key"), new Text("value_value"), mapFieldSchema);
    record1.setFieldValue("b", orcMap1);

    // Key same, value differ
    OrcStruct record2 = (OrcStruct) OrcStruct.createValue(schema);
    record2.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap2 = createOrcMap(new Text("key"), new Text("value_value"), mapFieldSchema);
    record2.setFieldValue("b", orcMap2);

    // Same as base
    OrcStruct record3 = (OrcStruct) OrcStruct.createValue(schema);
    record3.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap3 = createOrcMap(new Text("key"), new Text("value"), mapFieldSchema);
    record3.setFieldValue("b", orcMap3);

    // Differ in other field.
    OrcStruct record4 = (OrcStruct) OrcStruct.createValue(schema);
    record4.setFieldValue("a", new IntWritable(2));
    record4.setFieldValue("b", orcMap);

    OrcKey orcKey0 = new OrcKey();
    orcKey0.key = record0;
    OrcKey orcKey1 = new OrcKey();
    orcKey1.key = record1;
    OrcKey orcKey2 = new OrcKey();
    orcKey2.key = record2;
    OrcKey orcKey3 = new OrcKey();
    orcKey3.key = record3;
    OrcKey orcKey4 = new OrcKey();
    orcKey4.key = record4;

    Assert.assertTrue(comparator.compare(orcKey0, orcKey1) < 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey2) > 0);
    Assert.assertTrue(comparator.compare(orcKey2, orcKey3) > 0);
    Assert.assertTrue(comparator.compare(orcKey0, orcKey3) == 0);
    Assert.assertTrue(comparator.compare(orcKey0, orcKey4) < 0);
  }

  private OrcMap createOrcMap(Text key, Text value, TypeDescription schema) {
    TreeMap map = new TreeMap<Text, Text>();
    map.put(key, value);
    OrcMap result = new OrcMap(schema);
    result.putAll(map);
    return result;
  }

  /**
   * Create a {@link OrcList} repeating the given parameter inside the list for multiple times.
   */
  private OrcList createOrcList(int element, TypeDescription schema, int num) {
    OrcList result = new OrcList(schema);
    for (int i = 0; i < num; i++) {
      result.add(new IntWritable(element));
    }
    return result;
  }

  private OrcUnion createOrcUnion()
}
