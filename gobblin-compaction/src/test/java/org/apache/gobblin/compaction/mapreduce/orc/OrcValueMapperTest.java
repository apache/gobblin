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
import org.junit.Assert;
import org.testng.annotations.Test;


public class OrcValueMapperTest {
  @Test
  public void testIsEvolutionValid() {
    TypeDescription schema_1 = TypeDescription.fromString("struct<i:int,j:int,k:int>");
    TypeDescription schema_2 = TypeDescription.fromString("struct<i:int,j:int,k:bigint>");
    TypeDescription schema_3 = TypeDescription.fromString("struct<i:int,j:int,k:tinyint>");
    TypeDescription schema_4 = TypeDescription.fromString("struct<i:int,j:int>");
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_1, schema_2));
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_1, schema_3));
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_1, schema_4));
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_4, schema_1));
  }
}