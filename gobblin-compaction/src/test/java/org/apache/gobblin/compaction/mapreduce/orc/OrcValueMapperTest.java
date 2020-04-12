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

import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.mapred.OrcStruct;
import org.junit.Assert;
import org.testng.annotations.Test;


public class OrcValueMapperTest {
  @Test
  public void testIsEvolutionValid() {
    TypeDescription schema_1 = TypeDescription.fromString("struct<i:int,j:int,k:int>");
    TypeDescription schema_2 = TypeDescription.fromString("struct<i:int,j:int,k:bigint>");
    TypeDescription schema_3 = TypeDescription.fromString("struct<i:int,j:int,k:tinyint>");
    TypeDescription schema_4 = TypeDescription.fromString("struct<i:int,j:int>");
    Assert.assertTrue(OrcUtils.isEvolutionValid(schema_1, schema_2));
    Assert.assertTrue(OrcUtils.isEvolutionValid(schema_1, schema_3));
    Assert.assertTrue(OrcUtils.isEvolutionValid(schema_1, schema_4));
    Assert.assertTrue(OrcUtils.isEvolutionValid(schema_4, schema_1));
  }

  @Test
  public void testUpConvertOrcStruct(){
    OrcValueMapper mapper = new OrcValueMapper();

    // Basic case.
    TypeDescription baseStructSchema = TypeDescription.fromString("struct<a:int,b:string>");
    OrcStruct baseStruct = (OrcStruct) OrcStruct.createValue(baseStructSchema);
    TypeDescription evolved_baseStructSchema = TypeDescription.fromString("struct<a:int,b:string,c:int>");
    OrcStruct evolvedStruct = (OrcStruct) OrcStruct.createValue(evolved_baseStructSchema);
    OrcStruct resultStruct = mapper.upConvertOrcStruct(baseStruct, evolved_baseStructSchema);
    Assert.assertEquals(resultStruct.getSchema(), evolved_baseStructSchema);

    // Base case: Reverse direction.
    resultStruct = mapper.upConvertOrcStruct(evolvedStruct, baseStructSchema);
    Assert.assertEquals(resultStruct.getSchema(), baseStructSchema);

    // Simple Nested: List/Map/Union/Struct within Struct.
    TypeDescription listInStructSchema = TypeDescription.fromString("struct<a:array<struct<a:int,b:string>>>");
    OrcStruct listInStruct = (OrcStruct) OrcStruct.createValue(listInStructSchema);
    TypeDescription evolved_listInStructSchema = TypeDescription.fromString("struct<a:array<struct<a:int,b:string,c:string>>>");
    OrcStruct evolved_listInStruct = (OrcStruct) OrcStruct.createValue(evolved_listInStructSchema);
    resultStruct = mapper.upConvertOrcStruct(listInStruct, evolved_listInStructSchema);
    Assert.assertEquals(resultStruct.getSchema(), evolved_listInStructSchema);
    resultStruct = mapper.upConvertOrcStruct(evolved_listInStruct, listInStructSchema);
    Assert.assertEquals(resultStruct.getSchema(), listInStructSchema);

    TypeDescription mapInStructSchema = TypeDescription.fromString("struct<a:map<string,int>>");
    OrcStruct mapInStruct = (OrcStruct) OrcStruct.createValue(mapInStructSchema);
    TypeDescription evolved_mapInStructSchema = TypeDescription.fromString("struct<a:map<string,bigint>>");
    OrcStruct evolved_mapInStruct = (OrcStruct) OrcStruct.createValue(evolved_mapInStructSchema);
    resultStruct = mapper.upConvertOrcStruct(mapInStruct, evolved_mapInStructSchema);
    Assert.assertEquals(resultStruct.getSchema(), evolved_mapInStructSchema);
    resultStruct = mapper.upConvertOrcStruct(evolved_mapInStruct, mapInStructSchema);
    // Evolution not valid, no up-conversion happened.
    try {
      resultStruct.getSchema().equals(evolved_mapInStructSchema);
    } catch (SchemaEvolution.IllegalEvolutionException ie) {
      Assert.assertTrue(true);
    }

    TypeDescription unionInStructSchema = TypeDescription.fromString("struct<a:uniontype<int,string>>");
    OrcStruct unionInStruct = (OrcStruct) OrcStruct.createValue(unionInStructSchema);
    TypeDescription evolved_unionInStructSchema = TypeDescription.fromString("struct<a:uniontype<bigint,string>>");
    resultStruct = mapper.upConvertOrcStruct(unionInStruct, evolved_unionInStructSchema);
    Assert.assertEquals(resultStruct.getSchema(), evolved_unionInStructSchema);

    // Complex: List<Struct> within struct among others and evolution happens on multiple places.
    TypeDescription complex_1 = TypeDescription.fromString("struct<a:array<struct<a:string,b:int>>,b:struct<a:uniontype<int,string>>>");
    OrcStruct complex_struct = (OrcStruct) OrcStruct.createValue(complex_1);
    TypeDescription evolved_complex_1 = TypeDescription.fromString("struct<a:array<struct<a:string,b:int,c:string>>,b:struct<a:uniontype<bigint,string>,b:int>>");
    resultStruct = mapper.upConvertOrcStruct(complex_struct, evolved_complex_1);
    Assert.assertEquals(resultStruct.getSchema(), evolved_complex_1);
  }
}