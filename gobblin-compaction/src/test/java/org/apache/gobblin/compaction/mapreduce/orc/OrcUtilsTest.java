package org.apache.gobblin.compaction.mapreduce.orc;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class OrcUtilsTest {

  @Test
  public void testRandomFillOrcStructWithAnySchema() {
    // 1. Basic case
    TypeDescription schema_1 = TypeDescription.fromString("struct<i:int,j:int,k:int>");
    OrcStruct expectedStruct = (OrcStruct) OrcStruct.createValue(schema_1);
    expectedStruct.setFieldValue("i", new IntWritable(3));
    expectedStruct.setFieldValue("j", new IntWritable(3));
    expectedStruct.setFieldValue("k", new IntWritable(3));

    OrcStruct actualStruct = (OrcStruct) OrcStruct.createValue(schema_1);
    OrcUtils.orcStructFillerWithFixedValue(actualStruct, schema_1, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct);

    TypeDescription schema_2 = TypeDescription.fromString("struct<i:boolean,j:int,k:string>");
    expectedStruct = (OrcStruct) OrcStruct.createValue(schema_2);
    expectedStruct.setFieldValue("i", new BooleanWritable(false));
    expectedStruct.setFieldValue("j", new IntWritable(3));
    expectedStruct.setFieldValue("k", new Text(""));
    actualStruct = (OrcStruct) OrcStruct.createValue(schema_2);

    OrcUtils.orcStructFillerWithFixedValue(actualStruct, schema_2, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct);

    // 2. Some simple nested cases: struct within struct
    TypeDescription schema_3 = TypeDescription.fromString("struct<i:boolean,j:struct<i:boolean,j:int,k:string>>");
    OrcStruct expectedStruct_nested_1 = (OrcStruct) OrcStruct.createValue(schema_3);
    expectedStruct_nested_1.setFieldValue("i", new BooleanWritable(false));
    expectedStruct_nested_1.setFieldValue("j", expectedStruct);
    actualStruct = (OrcStruct) OrcStruct.createValue(schema_3);

    OrcUtils.orcStructFillerWithFixedValue(actualStruct, schema_3, 3, "", false);
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

    OrcUtils.orcStructFillerWithFixedValue(actualStruct, schema_4, 3, "", false);
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

    OrcUtils.orcStructFillerWithFixedValue(actualStruct, schema_5, 3, "", false);
    Assert.assertEquals(actualStruct, expectedStruct_nested_1);
  }
}