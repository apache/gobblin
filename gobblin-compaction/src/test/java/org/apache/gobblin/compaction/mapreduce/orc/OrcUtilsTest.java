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

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.compaction"})
public class OrcUtilsTest {
  final int intValue1 = 10;
  final String stringValue1 = "testString1";
  final int intValue2 = 20;
  final String stringValue2 = "testString2";
  final int intValue3 = 30;
  final String stringValue3 = "testString3";
  final boolean boolValue = true;

  @Test
  public void testRandomFillOrcStructWithAnySchema() {
    // 1. Basic case
    TypeDescription schema_1 = TypeDescription.fromString("struct<i:int,j:int,k:int>");
    OrcStruct expectedStruct = (OrcStruct) OrcStruct.createValue(schema_1);
    expectedStruct.setFieldValue("i", new IntWritable(3));
    expectedStruct.setFieldValue("j", new IntWritable(3));
    expectedStruct.setFieldValue("k", new IntWritable(3));

    OrcStruct actualStruct = (OrcStruct) OrcStruct.createValue(schema_1);
    OrcTestUtils.fillOrcStructWithFixedValue(actualStruct, schema_1, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct);

    TypeDescription schema_2 = TypeDescription.fromString("struct<i:boolean,j:int,k:string>");
    expectedStruct = (OrcStruct) OrcStruct.createValue(schema_2);
    expectedStruct.setFieldValue("i", new BooleanWritable(false));
    expectedStruct.setFieldValue("j", new IntWritable(3));
    expectedStruct.setFieldValue("k", new Text(""));
    actualStruct = (OrcStruct) OrcStruct.createValue(schema_2);

    OrcTestUtils.fillOrcStructWithFixedValue(actualStruct, schema_2, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct);

    // 2. Some simple nested cases: struct within struct
    TypeDescription schema_3 = TypeDescription.fromString("struct<i:boolean,j:struct<i:boolean,j:int,k:string>>");
    OrcStruct expectedStruct_nested_1 = (OrcStruct) OrcStruct.createValue(schema_3);
    expectedStruct_nested_1.setFieldValue("i", new BooleanWritable(false));
    expectedStruct_nested_1.setFieldValue("j", expectedStruct);
    actualStruct = (OrcStruct) OrcStruct.createValue(schema_3);

    OrcTestUtils.fillOrcStructWithFixedValue(actualStruct, schema_3, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct_nested_1);

    // 3. array of struct within struct
    TypeDescription schema_4 = TypeDescription.fromString("struct<i:boolean,j:array<struct<i:boolean,j:int,k:string>>>");
    // Note that this will not create any elements in the array.
    expectedStruct_nested_1 = (OrcStruct) OrcStruct.createValue(schema_4);
    expectedStruct_nested_1.setFieldValue("i", new BooleanWritable(false));
    OrcList list = new OrcList(schema_2, 1);
    list.add(expectedStruct);
    expectedStruct_nested_1.setFieldValue("j", list);

    // Constructing actualStruct: make sure the list is non-Empty. There's any meaningful value within placeholder struct.
    actualStruct = (OrcStruct) OrcStruct.createValue(schema_4);
    OrcList placeHolderList = new OrcList(schema_2, 1);
    OrcStruct placeHolderStruct = (OrcStruct) OrcStruct.createValue(schema_2);
    placeHolderList.add(placeHolderStruct);
    actualStruct.setFieldValue("j", placeHolderList);

    OrcTestUtils.fillOrcStructWithFixedValue(actualStruct, schema_4, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct_nested_1);

    // 4. union of struct within struct
    TypeDescription schema_5 = TypeDescription.fromString("struct<i:boolean,j:uniontype<struct<i:boolean,j:int,k:string>>>");
    expectedStruct_nested_1 = (OrcStruct) OrcStruct.createValue(schema_5);
    expectedStruct_nested_1.setFieldValue("i", new BooleanWritable(false));
    OrcUnion union = new OrcUnion(schema_2);
    union.set(0, expectedStruct);
    expectedStruct_nested_1.setFieldValue("j", union);

    // Construct actualStruct: make sure there's a struct-placeholder within the union.
    actualStruct = (OrcStruct) OrcStruct.createValue(schema_5);
    OrcUnion placeHolderUnion = new OrcUnion(schema_2);
    placeHolderUnion.set(0, placeHolderStruct);
    actualStruct.setFieldValue("j", placeHolderUnion);

    OrcTestUtils.fillOrcStructWithFixedValue(actualStruct, schema_5, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct_nested_1);
  }

  @Test
  public void testUpConvertSimpleOrcStruct() {
    // Basic case, all primitives, newly added value will be set to null
    TypeDescription baseStructSchema = TypeDescription.fromString("struct<a:int,b:string>");
    // This would be re-used in the following tests as the actual record using the schema.
    OrcStruct baseStruct = (OrcStruct) OrcStruct.createValue(baseStructSchema);
    // Fill in the baseStruct with specified value.
    OrcTestUtils.fillOrcStructWithFixedValue(baseStruct, baseStructSchema, intValue1, stringValue1, boolValue);
    TypeDescription evolved_baseStructSchema = TypeDescription.fromString("struct<a:int,b:string,c:int>");
    OrcStruct evolvedStruct = (OrcStruct) OrcStruct.createValue(evolved_baseStructSchema);
    // This should be equivalent to deserialize(baseStruct).serialize(evolvedStruct, evolvedSchema);
    OrcUtils.upConvertOrcStruct(baseStruct, evolvedStruct, evolved_baseStructSchema);
    // Check if all value in baseStruct is populated and newly created column in evolvedStruct is filled with null.
    Assert.assertEquals(((IntWritable) evolvedStruct.getFieldValue("a")).get(), intValue1);
    Assert.assertEquals(evolvedStruct.getFieldValue("b").toString(), stringValue1);
    Assert.assertNull(evolvedStruct.getFieldValue("c"));

    // Base case: Reverse direction, which is column projection on top-level columns.
    OrcStruct baseStruct_shadow = (OrcStruct) OrcStruct.createValue(baseStructSchema);
    OrcUtils.upConvertOrcStruct(evolvedStruct, baseStruct_shadow, baseStructSchema);
    Assert.assertEquals(baseStruct, baseStruct_shadow);
  }

  @Test
  public void testUpConvertOrcStructOfList() {
    // Simple Nested: List within Struct.
    // The element type of list contains a new field.
    // Prepare two ListInStructs with different size ( the list field contains different number of members)
    TypeDescription structOfListSchema = TypeDescription.fromString("struct<a:array<struct<a:int,b:string>>>");
    OrcStruct structOfList = (OrcStruct) OrcUtils.createValueRecursively(structOfListSchema);

    //Create an OrcList instance with two entries
    TypeDescription innerStructSchema = TypeDescription.createStruct().addField("a", TypeDescription.createInt())
        .addField("b", TypeDescription.createString());
    OrcStruct innerStruct1 = new OrcStruct(innerStructSchema);
    innerStruct1.setFieldValue("a", new IntWritable(intValue1));
    innerStruct1.setFieldValue("b", new Text(stringValue1));

    OrcStruct innerStruct2 = new OrcStruct(innerStructSchema);
    innerStruct2.setFieldValue("a", new IntWritable(intValue2));
    innerStruct2.setFieldValue("b", new Text(stringValue2));

    TypeDescription listSchema = TypeDescription.createList(innerStructSchema);
    OrcList orcList = new OrcList(listSchema);
    orcList.add(innerStruct1);
    orcList.add(innerStruct2);
    structOfList.setFieldValue("a", orcList);

    TypeDescription evolvedStructOfListSchema =
        TypeDescription.fromString("struct<a:array<struct<a:int,b:string,c:int>>>");
    OrcStruct evolvedStructOfList = (OrcStruct) OrcUtils.createValueRecursively(evolvedStructOfListSchema);
    // Convert and verify contents.
    OrcUtils.upConvertOrcStruct(structOfList, evolvedStructOfList, evolvedStructOfListSchema);
    Assert.assertEquals(
        ((IntWritable) ((OrcStruct) ((OrcList) evolvedStructOfList.getFieldValue("a")).get(0)).getFieldValue("a"))
            .get(), intValue1);
    Assert.assertEquals(
        ((OrcStruct) ((OrcList) evolvedStructOfList.getFieldValue("a")).get(0)).getFieldValue("b").toString(),
        stringValue1);
    Assert.assertNull((((OrcStruct) ((OrcList) evolvedStructOfList.getFieldValue("a")).get(0)).getFieldValue("c")));
    Assert.assertEquals(
        ((IntWritable) ((OrcStruct) ((OrcList) evolvedStructOfList.getFieldValue("a")).get(1)).getFieldValue("a"))
            .get(), intValue2);
    Assert.assertEquals(
        ((OrcStruct) ((OrcList) evolvedStructOfList.getFieldValue("a")).get(1)).getFieldValue("b").toString(),
        stringValue2);
    Assert.assertNull((((OrcStruct) ((OrcList) evolvedStructOfList.getFieldValue("a")).get(1)).getFieldValue("c")));

    //Create a list in source OrcStruct with 3 elements
    structOfList = (OrcStruct) OrcUtils.createValueRecursively(structOfListSchema, 3);
    OrcTestUtils.fillOrcStructWithFixedValue(structOfList, structOfListSchema, intValue1, stringValue1, boolValue);
    Assert.assertNotEquals(((OrcList) structOfList.getFieldValue("a")).size(),
        ((OrcList) evolvedStructOfList.getFieldValue("a")).size());
    OrcUtils.upConvertOrcStruct(structOfList, evolvedStructOfList, evolvedStructOfListSchema);
    Assert.assertEquals(((OrcList) evolvedStructOfList.getFieldValue("a")).size(), 3);
    // Original has list.size()=0, target has list.size() = 1
    ((OrcList) structOfList.getFieldValue("a")).clear();
    OrcUtils.upConvertOrcStruct(structOfList, evolvedStructOfList, evolvedStructOfListSchema);
    Assert.assertEquals(((OrcList) evolvedStructOfList.getFieldValue("a")).size(), 0);
  }

  @Test
  public void testUpConvertOrcStructOfMap() {
    // Map within Struct, contains a type-widening in the map-value type.
    TypeDescription structOfMapSchema = TypeDescription.fromString("struct<a:map<string,int>>");
    OrcStruct structOfMap = (OrcStruct) OrcStruct.createValue(structOfMapSchema);
    TypeDescription mapSchema = TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createInt());
    OrcMap testMap = new OrcMap(mapSchema);
    //Add dummy entries to initialize the testMap. The actual keys and values will be set later.
    testMap.put(new Text(stringValue1), new IntWritable(intValue1));
    testMap.put(new Text(stringValue2), new IntWritable(intValue2));
    structOfMap.setFieldValue("a", testMap);
    // Create the target struct with evolved schema
    TypeDescription evolvedStructOfMapSchema = TypeDescription.fromString("struct<a:map<string,bigint>>");
    OrcStruct evolvedStructOfMap = (OrcStruct) OrcStruct.createValue(evolvedStructOfMapSchema);
    OrcMap evolvedMap =
        new OrcMap(TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createInt()));
    //Initialize a map
    evolvedMap.put(new Text(""), new LongWritable());
    evolvedStructOfMap.setFieldValue("a", evolvedMap);
    // convert and verify: Type-widening is correct, and size of output file is correct.
    OrcUtils.upConvertOrcStruct(structOfMap, evolvedStructOfMap, evolvedStructOfMapSchema);

    Assert.assertEquals(((OrcMap) evolvedStructOfMap.getFieldValue("a")).get(new Text(stringValue1)),
        new LongWritable(intValue1));
    Assert.assertEquals(((OrcMap) evolvedStructOfMap.getFieldValue("a")).get(new Text(stringValue2)),
        new LongWritable(intValue2));
    Assert.assertEquals(((OrcMap) evolvedStructOfMap.getFieldValue("a")).size(), 2);
    // re-use the same object but the source struct has fewer member in the map entry.
    testMap.put(new Text(stringValue3), new IntWritable(intValue3));
    // sanity check
    Assert.assertEquals(((OrcMap) structOfMap.getFieldValue("a")).size(), 3);
    OrcUtils.upConvertOrcStruct(structOfMap, evolvedStructOfMap, evolvedStructOfMapSchema);
    Assert.assertEquals(((OrcMap) evolvedStructOfMap.getFieldValue("a")).size(), 3);
    Assert.assertEquals(((OrcMap) evolvedStructOfMap.getFieldValue("a")).get(new Text(stringValue1)),
        new LongWritable(intValue1));
    Assert.assertEquals(((OrcMap) evolvedStructOfMap.getFieldValue("a")).get(new Text(stringValue2)),
        new LongWritable(intValue2));
    Assert.assertEquals(((OrcMap) evolvedStructOfMap.getFieldValue("a")).get(new Text(stringValue3)),
        new LongWritable(intValue3));
  }

  @Test
  public void testUpConvertOrcStructOfUnion() {
    // Union in struct, type widening within the union's member field.
    TypeDescription unionInStructSchema = TypeDescription.fromString("struct<a:uniontype<int,string>>");
    OrcStruct unionInStruct = (OrcStruct) OrcStruct.createValue(unionInStructSchema);
    OrcUnion placeHolderUnion = new OrcUnion(TypeDescription.fromString("uniontype<int,string>"));
    placeHolderUnion.set(0, new IntWritable(1));
    unionInStruct.setFieldValue("a", placeHolderUnion);
    OrcTestUtils.fillOrcStructWithFixedValue(unionInStruct, unionInStructSchema, intValue1, stringValue1, boolValue);
    // Create new structWithUnion
    TypeDescription evolved_unionInStructSchema = TypeDescription.fromString("struct<a:uniontype<bigint,string>>");
    OrcStruct evolvedUnionInStruct = (OrcStruct) OrcStruct.createValue(evolved_unionInStructSchema);
    OrcUnion evolvedPlaceHolderUnion = new OrcUnion(TypeDescription.fromString("uniontype<bigint,string>"));
    evolvedPlaceHolderUnion.set(0, new LongWritable(1L));
    evolvedUnionInStruct.setFieldValue("a", evolvedPlaceHolderUnion);
    OrcUtils.upConvertOrcStruct(unionInStruct, evolvedUnionInStruct, evolved_unionInStructSchema);
    // Check in the tag 0(Default from value-filler) within evolvedUnionInStruct, the value is becoming type-widened with correct value.
    Assert.assertEquals(((OrcUnion) evolvedUnionInStruct.getFieldValue("a")).getTag(), 0);
    Assert.assertEquals(((OrcUnion) evolvedUnionInStruct.getFieldValue("a")).getObject(), new LongWritable(intValue1));
    // Check the case when union field is created in different tag.

    // Complex: List<Struct> within struct among others and evolution happens on multiple places, also type-widening in deeply nested level.
    TypeDescription complexOrcSchema =
        TypeDescription.fromString("struct<a:array<struct<a:string,b:int>>,b:struct<a:uniontype<int,string>>>");
    OrcStruct complexOrcStruct = (OrcStruct) OrcUtils.createValueRecursively(complexOrcSchema);
    OrcTestUtils.fillOrcStructWithFixedValue(complexOrcStruct, complexOrcSchema, intValue1, stringValue1, boolValue);
    TypeDescription evolvedComplexOrcSchema = TypeDescription
        .fromString("struct<a:array<struct<a:string,b:bigint,c:string>>,b:struct<a:uniontype<bigint,string>,b:int>>");
    OrcStruct evolvedComplexStruct = (OrcStruct) OrcUtils.createValueRecursively(evolvedComplexOrcSchema);
    OrcTestUtils
        .fillOrcStructWithFixedValue(evolvedComplexStruct, evolvedComplexOrcSchema, intValue1, stringValue1, boolValue);
    // Check if new columns are assigned with null value and type widening is working fine.
    OrcUtils.upConvertOrcStruct(complexOrcStruct, evolvedComplexStruct, evolvedComplexOrcSchema);
    Assert
        .assertEquals(((OrcStruct)((OrcList)evolvedComplexStruct.getFieldValue("a")).get(0)).getFieldValue("b"), new LongWritable(intValue1));
    Assert.assertNull(((OrcStruct)((OrcList)evolvedComplexStruct.getFieldValue("a")).get(0)).getFieldValue("c"));
    Assert.assertEquals(((OrcUnion) ((OrcStruct)evolvedComplexStruct.getFieldValue("b")).getFieldValue("a")).getObject(), new LongWritable(intValue1));
    Assert.assertNull(((OrcStruct)evolvedComplexStruct.getFieldValue("b")).getFieldValue("b"));
  }

  @Test
  public void testNestedWithinUnionWithDiffTag() {
    // Construct union type with different tag for the src object dest object, check if up-convert happens correctly.
    TypeDescription structInUnionAsStruct = TypeDescription.fromString("struct<a:uniontype<struct<a:int,b:string>,int>>");
    OrcStruct structInUnionAsStructObject = (OrcStruct) OrcUtils.createValueRecursively(structInUnionAsStruct);
    OrcTestUtils
        .fillOrcStructWithFixedValue(structInUnionAsStructObject, structInUnionAsStruct, 0, intValue1, stringValue1, boolValue);
    Assert.assertEquals(((OrcStruct)((OrcUnion)structInUnionAsStructObject.getFieldValue("a")).getObject())
        .getFieldValue("a"), new IntWritable(intValue1));

    OrcStruct structInUnionAsStructObject_2 = (OrcStruct) OrcUtils.createValueRecursively(structInUnionAsStruct);
    OrcTestUtils
        .fillOrcStructWithFixedValue(structInUnionAsStructObject_2, structInUnionAsStruct, 1, intValue1, stringValue1, boolValue);
    Assert.assertEquals(((OrcUnion)structInUnionAsStructObject_2.getFieldValue("a")).getObject(), new IntWritable(intValue1));

    // Create a new record container, do up-convert twice and check if the value is propagated properly.
    OrcStruct container = (OrcStruct) OrcUtils.createValueRecursively(structInUnionAsStruct);
    OrcUtils.upConvertOrcStruct(structInUnionAsStructObject, container, structInUnionAsStruct);
    Assert.assertEquals(structInUnionAsStructObject, container);

    OrcUtils.upConvertOrcStruct(structInUnionAsStructObject_2, container, structInUnionAsStruct);
    Assert.assertEquals(structInUnionAsStructObject_2, container);
  }

  /**
   * This test mostly target at the following case:
   * Schema: struct<a:array<struct<a:int,b:int>>>
   * field a was set to null by one call of "upConvertOrcStruct", but the subsequent call should still have the nested
   * field filled.
   */
  public void testNestedFieldSequenceSet() {
    TypeDescription schema = TypeDescription.fromString("struct<a:array<struct<a:int,b:int>>>");
    OrcStruct struct = (OrcStruct) OrcUtils.createValueRecursively(schema);
    OrcTestUtils.fillOrcStructWithFixedValue(struct, schema, 1, "test", true);
    OrcStruct structWithEmptyArray = (OrcStruct) OrcUtils.createValueRecursively(schema);
    OrcTestUtils.fillOrcStructWithFixedValue(structWithEmptyArray, schema, 1, "test", true);
    structWithEmptyArray.setFieldValue("a", null);
    OrcUtils.upConvertOrcStruct(structWithEmptyArray, struct, schema);
    Assert.assertEquals(struct, structWithEmptyArray);

    OrcStruct struct_2 = (OrcStruct) OrcUtils.createValueRecursively(schema);
    OrcTestUtils.fillOrcStructWithFixedValue(struct_2, schema, 2, "test", true);
    OrcUtils.upConvertOrcStruct(struct_2, struct, schema);
    Assert.assertEquals(struct, struct_2);
  }

  /**
   * Just a sanity test for column project, should be no difference from other cases when provided reader schema.
   */
  @Test
  public void testOrcStructProjection() {
    TypeDescription originalSchema = TypeDescription.fromString("struct<a:struct<a:int,b:int>,b:struct<c:int,d:int>,c:int>");
    OrcStruct originalStruct = (OrcStruct) OrcUtils.createValueRecursively(originalSchema);
    OrcTestUtils.fillOrcStructWithFixedValue(originalStruct, originalSchema, intValue1, stringValue1, boolValue);

    TypeDescription projectedSchema = TypeDescription.fromString("struct<a:struct<b:int>,b:struct<c:int>>");
    OrcStruct projectedStructExpectedValue = (OrcStruct) OrcUtils.createValueRecursively(projectedSchema);
    OrcTestUtils
        .fillOrcStructWithFixedValue(projectedStructExpectedValue, projectedSchema, intValue1, stringValue1, boolValue);
    OrcStruct projectColumnStruct = (OrcStruct) OrcUtils.createValueRecursively(projectedSchema);
    OrcUtils.upConvertOrcStruct(originalStruct, projectColumnStruct, projectedSchema);
    Assert.assertEquals(projectColumnStruct, projectedStructExpectedValue);
  }

  @Test
  public void complexTypeEligibilityCheck() {
    TypeDescription struct_array_0 = TypeDescription.fromString("struct<first:array<int>,second:int>");
    TypeDescription struct_array_1 = TypeDescription.fromString("struct<first:array<int>,second:int>");
    Assert.assertTrue(OrcUtils.eligibleForUpConvert(struct_array_0, struct_array_1));
    TypeDescription struct_array_2 = TypeDescription.fromString("struct<first:array<string>,second:int>");
    Assert.assertFalse(OrcUtils.eligibleForUpConvert(struct_array_0, struct_array_2));

    TypeDescription struct_map_0 = TypeDescription.fromString("struct<first:map<string,string>,second:int>");
    TypeDescription struct_map_1 = TypeDescription.fromString("struct<first:map<string,string>,second:int>");
    TypeDescription struct_map_2 = TypeDescription.fromString("struct<first:map<string,int>,second:int>");
    TypeDescription struct_map_3 = TypeDescription.fromString("struct<second:int>");
    Assert.assertTrue(OrcUtils.eligibleForUpConvert(struct_map_0, struct_map_1));
    Assert.assertFalse(OrcUtils.eligibleForUpConvert(struct_map_0, struct_map_2));
    Assert.assertTrue(OrcUtils.eligibleForUpConvert(struct_map_0, struct_map_3));
  }

  public void testSchemaContains() {
    // Simple case.
    TypeDescription struct_0 = TypeDescription.fromString("struct<a:int,b:int>");
    TypeDescription struct_1 = TypeDescription.fromString("struct<a:int>");
    Assert.assertTrue(OrcUtils.eligibleForUpConvert(struct_0, struct_1));

    // Nested schema case.
    TypeDescription struct_2 = TypeDescription.fromString("struct<a:struct<a:int,b:int>,b:struct<c:int,d:int>,c:int>");
    TypeDescription struct_3 = TypeDescription.fromString("struct<a:struct<a:int>,b:struct<c:int>,c:int>");
    Assert.assertTrue(OrcUtils.eligibleForUpConvert(struct_2, struct_3));

    // Negative case.
    TypeDescription struct_4 = TypeDescription.fromString("struct<a:struct<a:int,b:int>,b:struct<c:int,d:int>,c:int>");
    TypeDescription struct_5 = TypeDescription.fromString("struct<a:struct<a:int>,b:struct<c:int>,d:int>");
    Assert.assertFalse(OrcUtils.eligibleForUpConvert(struct_4, struct_5));
    TypeDescription struct_6 = TypeDescription.fromString("struct<a:struct<a:int>,b:struct<e:int>,c:int>");
    Assert.assertFalse(OrcUtils.eligibleForUpConvert(struct_4, struct_6));

    // Cases when target schema contains more
    TypeDescription struct_7 = TypeDescription.fromString("struct<a:struct<a:int>,b:struct<e:int,f:int>,c:int>");
    Assert.assertTrue(OrcUtils.eligibleForUpConvert(struct_6, struct_7));

    // Negative case when target schema contains more but not all of the owning schema are there in the target schema.
    // Note that struct_8 has a field "a.x".
    TypeDescription struct_8 = TypeDescription.fromString("struct<a:struct<x:int>,b:struct<e:int>,c:int>");
    TypeDescription struct_9 = TypeDescription.fromString("struct<a:struct<a:int>,b:struct<e:int,f:int>,c:int>");
    Assert.assertFalse(OrcUtils.eligibleForUpConvert(struct_8, struct_9));
  }
}