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
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Mainly to test {@link OrcKeyComparator} is behaving as expected when it is comparing two {@link OrcStruct}.
 * It covers basic(primitive) type of {@link OrcStruct} and those contain complex type (MAP, LIST, UNION, Struct)
 *
 * Reference: https://orc.apache.org/docs/types.html
 */
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

    OrcStruct record0 = createSimpleOrcStruct(schema, 1, 2);
    OrcStruct record1 = createSimpleOrcStruct(schema, 3, 4);
    OrcStruct record2 = createSimpleOrcStruct(schema, 3, 4);

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
  public void testComplexRecordArray() throws Exception {
    OrcKeyComparator comparator = new OrcKeyComparator();
    Configuration conf = new Configuration();

    TypeDescription listSchema = TypeDescription.createList(TypeDescription.createString());
    TypeDescription schema =
        TypeDescription.createStruct().addField("a", TypeDescription.createInt()).addField("b", listSchema);

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
  public void testComplexRecordMap() throws Exception {
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
    OrcMap orcMap = createSimpleOrcMap(new Text("key"), new Text("value"), mapFieldSchema);
    record0.setFieldValue("b", orcMap);

    // key value both differ
    OrcStruct record1 = (OrcStruct) OrcStruct.createValue(schema);
    record1.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap1 = createSimpleOrcMap(new Text("key_key"), new Text("value_value"), mapFieldSchema);
    record1.setFieldValue("b", orcMap1);

    // Key same, value differ
    OrcStruct record2 = (OrcStruct) OrcStruct.createValue(schema);
    record2.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap2 = createSimpleOrcMap(new Text("key"), new Text("value_value"), mapFieldSchema);
    record2.setFieldValue("b", orcMap2);

    // Same as base
    OrcStruct record3 = (OrcStruct) OrcStruct.createValue(schema);
    record3.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap3 = createSimpleOrcMap(new Text("key"), new Text("value"), mapFieldSchema);
    record3.setFieldValue("b", orcMap3);

    // Differ in other field.
    OrcStruct record4 = (OrcStruct) OrcStruct.createValue(schema);
    record4.setFieldValue("a", new IntWritable(2));
    record4.setFieldValue("b", orcMap);

    // Record with map containing multiple entries but inserted in different order.
    OrcStruct record6 = (OrcStruct) OrcStruct.createValue(schema);
    record6.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap6 = createSimpleOrcMap(new Text("key"), new Text("value"), mapFieldSchema);
    orcMap6.put(new Text("keyLater"), new Text("valueLater"));
    record6.setFieldValue("b", orcMap6);

    OrcStruct record7 = (OrcStruct) OrcStruct.createValue(schema);
    record7.setFieldValue("a", new IntWritable(1));
    OrcMap orcMap7 = createSimpleOrcMap(new Text("keyLater"), new Text("valueLater"), mapFieldSchema);
    orcMap7.put(new Text("key"), new Text("value"));
    record7.setFieldValue("b", orcMap7);

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

    OrcKey orcKey6 = new OrcKey();
    orcKey6.key = record6;
    OrcKey orcKey7 = new OrcKey();
    orcKey7.key = record7;

    Assert.assertTrue(comparator.compare(orcKey0, orcKey1) < 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey2) > 0);
    Assert.assertTrue(comparator.compare(orcKey2, orcKey3) > 0);
    Assert.assertTrue(comparator.compare(orcKey0, orcKey3) == 0);
    Assert.assertTrue(comparator.compare(orcKey0, orcKey4) < 0);
    Assert.assertTrue(comparator.compare(orcKey6, orcKey7) == 0);
  }

  // Test comparison for union containing complex types and nested record inside.
  // Schema: struct<a:int,
  //                b:uniontype<int,
  //                            array<string>,
  //                            struct<x:int,y:int>
  //                            >
  //                >
  @Test
  public void testComplexRecordUnion() throws Exception {
    OrcKeyComparator comparator = new OrcKeyComparator();
    Configuration conf = new Configuration();

    TypeDescription listSchema = TypeDescription.createList(TypeDescription.createString());

    TypeDescription nestedRecordSchema = TypeDescription.createStruct()
        .addField("x", TypeDescription.createInt())
        .addField("y", TypeDescription.createInt());

    TypeDescription unionSchema = TypeDescription.createUnion()
        .addUnionChild(TypeDescription.createInt())
        .addUnionChild(listSchema)
        .addUnionChild(nestedRecordSchema);

    TypeDescription schema =
        TypeDescription.createStruct()
            .addField("a", TypeDescription.createInt())
            .addField("b", unionSchema);

    conf.set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), schema.toString());
    Assert.assertEquals(conf.get(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()), schema.toString());
    comparator.setConf(conf);

    // base record
    OrcStruct record0 = (OrcStruct) OrcStruct.createValue(schema);
    record0.setFieldValue("a", new IntWritable(1));
    OrcStruct nestedRecord0 = createSimpleOrcStruct(nestedRecordSchema, 1, 2);
    OrcUnion orcUnion0 = createOrcUnion(unionSchema, nestedRecord0);
    record0.setFieldValue("b", orcUnion0);

    // same content as base record in diff objects.
    OrcStruct record1 = (OrcStruct) OrcStruct.createValue(schema);
    record1.setFieldValue("a", new IntWritable(1));
    OrcStruct nestedRecord1 = createSimpleOrcStruct(nestedRecordSchema, 1, 2);
    OrcUnion orcUnion1 = createOrcUnion(unionSchema, nestedRecord1);
    record1.setFieldValue("b", orcUnion1);

    // diff records inside union, record0 == record1 < 2
    OrcStruct record2 = (OrcStruct) OrcStruct.createValue(schema);
    record2.setFieldValue("a", new IntWritable(1));
    OrcStruct nestedRecord2 = createSimpleOrcStruct(nestedRecordSchema, 2, 2);
    OrcUnion orcUnion2 = createOrcUnion(unionSchema, nestedRecord2);
    record2.setFieldValue("b", orcUnion2);


    // differ in list inside union, record3 < record4 == record5
    OrcStruct record3 = (OrcStruct) OrcStruct.createValue(schema);
    record3.setFieldValue("a", new IntWritable(1));
    OrcList orcList3 = createOrcList(5, listSchema, 2);
    OrcUnion orcUnion3 = createOrcUnion(unionSchema, orcList3);
    record3.setFieldValue("b", orcUnion3);

    OrcStruct record4 = (OrcStruct) OrcStruct.createValue(schema);
    record4.setFieldValue("a", new IntWritable(1));
    OrcList orcList4 = createOrcList(6, listSchema, 2);
    OrcUnion orcUnion4 = createOrcUnion(unionSchema, orcList4);
    record4.setFieldValue("b", orcUnion4);

    OrcStruct record5 = (OrcStruct) OrcStruct.createValue(schema);
    record5.setFieldValue("a", new IntWritable(1));
    OrcList orcList5 = createOrcList(6, listSchema, 2);
    OrcUnion orcUnion5 = createOrcUnion(unionSchema, orcList5);
    record5.setFieldValue("b", orcUnion5);


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
    OrcKey orcKey5 = new OrcKey();
    orcKey5.key = record5;

    Assert.assertEquals(orcUnion0, orcUnion1);
    // Int value in orcKey2 is larger
    Assert.assertTrue(comparator.compare(orcKey0, orcKey2) < 0);
    Assert.assertTrue(comparator.compare(orcKey3, orcKey4) < 0 );
    Assert.assertTrue(comparator.compare(orcKey3, orcKey5) < 0);
    Assert.assertTrue(comparator.compare(orcKey4, orcKey5) == 0);
  }

  private OrcMap createSimpleOrcMap(Text key, Text value, TypeDescription schema) {
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

  private OrcUnion createOrcUnion(TypeDescription schema, WritableComparable value) {
    OrcUnion result = new OrcUnion(schema);
    result.set(0, value);
    return result;
  }

  private OrcStruct createSimpleOrcStruct(TypeDescription structSchema, int value1, int value2) {
    OrcStruct result = new OrcStruct(structSchema);
    result.setFieldValue(0, new IntWritable(value1));
    result.setFieldValue(1, new IntWritable(value2));
    return result;
  }
}
