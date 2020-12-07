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

package org.apache.gobblin.binary_creation;

import com.google.common.io.Files;
import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.orc.TypeDescription;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrcTestToolsTest {

  public DataTestTools orcTools = new OrcTestTools();;

  @Test
  public void test() throws Exception {

    String resourceName = "orcWriterTest";

    File tmpDir = Files.createTempDir();

    FileSystem fs = FileSystem.get(new Configuration());

    Path output = new Path(tmpDir.getAbsolutePath(), "test");

    orcTools.writeJsonResourceRecordsAsBinary(resourceName, null, output, null);

    Assert.assertTrue(orcTools.checkSameFilesAndRecords(orcTools.readAllRecordsInJsonResource(resourceName, null),
        orcTools.readAllRecordsInBinaryDirectory(fs, output), true, null, false));
  }

  @Test
  public void testSchemaToTypeInfoConversion() throws Exception {
    // Simple non-nested case:
    Schema avroSchema = SchemaBuilder.record("test")
        .fields()
        .name("id")
        .type()
        .intType()
        .noDefault()
        .name("timestamp")
        .type()
        .stringType()
        .noDefault()
        .endRecord();

    TypeInfo orcSchema = OrcTestTools.convertAvroSchemaToOrcSchema(avroSchema);
    String targetOrcSchemaString = "struct<id:int,timestamp:string>";
    Assert.assertEquals(targetOrcSchemaString, orcSchema.toString());

    // Nested case:
    avroSchema = SchemaBuilder.record("nested")
        .fields()
        .name("nestedId")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .name("timestamp")
        .type()
        .stringType()
        .noDefault()
        .endRecord();
    orcSchema = OrcTestTools.convertAvroSchemaToOrcSchema(avroSchema);
    TypeDescription targetTypeDescription = TypeDescription.createStruct()
        .addField("nestedId", TypeDescription.createList(TypeDescription.createString()))
        .addField("timestamp", TypeDescription.createString());
    Assert.assertEquals(orcSchema.toString().toLowerCase(), targetTypeDescription.toString().toLowerCase());
  }
}