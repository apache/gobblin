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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.apache.gobblin.binary_creation.AvroTestTools.*;


public class AvroTestToolsTest {

  @Test
  public void test() throws Exception {
    DataTestTools testTools = new AvroTestTools();

    String resourceName = "avroWriterTest";

    File tmpDir = Files.createTempDir();

    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path output = new Path(tmpDir.getAbsolutePath(), "test");

    testTools.writeJsonResourceRecordsAsBinary(resourceName, fs, output, null);

    Assert.assertTrue(testTools.checkSameFilesAndRecords(testTools.readAllRecordsInJsonResource(resourceName, null),
        testTools.readAllRecordsInBinaryDirectory(fs, output), false, null, true));
  }

  @Test
  public void testGenericRecordDataComparisonWithoutSchema() throws Exception {
    Schema avroSchema = (new Schema.Parser()).parse(
        "{\n" + "  \"namespace\": \"com.linkedin.compliance.test\",\n" + "  \"type\": \"record\",\n"
            + "  \"name\": \"SimpleTest\",\n" + "  \"fields\": [\n" + "    {\n" + "      \"name\": \"memberId\",\n"
            + "      \"type\": \"int\"\n" + "    },\n" + "    {\n" + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n" + "    }\n" + "  ]\n" + "}");

    Schema avroSchemaDiffInNamespace = (new Schema.Parser()).parse(
        "{\n" + "  \"namespace\": \"com.linkedin.whatever\",\n" + "  \"type\": \"record\",\n"
            + "  \"name\": \"SimpleTest\",\n" + "  \"fields\": [\n" + "    {\n" + "      \"name\": \"memberId\",\n"
            + "      \"type\": \"int\"\n" + "    },\n" + "    {\n" + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n" + "    }\n" + "  ]\n" + "}");

    Schema nullableSchema = (new Schema.Parser()).parse(
        "{\n" + "  \"namespace\": \"com.linkedin.compliance.test\",\n" + "  \"type\": \"record\",\n"
            + "  \"name\": \"SimpleTest\",\n" + "  \"fields\": [\n" + "    {\n" + "      \"name\": \"memberId\",\n"
            + "      \"type\": [\n" + "        \"null\",\n" + "        \"int\",\n" + "        \"string\"\n"
            + "      ]\n" + "    },\n" + "    {\n" + "      \"name\": \"name\",\n" + "      \"type\": \"string\"\n"
            + "    }\n" + "  ]\n" + "}");

    GenericRecordBuilder builder_0 = new GenericRecordBuilder(avroSchema);
    builder_0.set("memberId", "1");
    builder_0.set("name", "alice");
    GenericData.Record record_0 = builder_0.build();

    GenericRecordBuilder builder_1 = new GenericRecordBuilder(avroSchemaDiffInNamespace);
    builder_1.set("memberId", "1");
    builder_1.set("name", "alice");
    GenericData.Record record_1 = builder_1.build();

    GenericRecordBuilder builder_2 = new GenericRecordBuilder(avroSchemaDiffInNamespace);
    builder_2.set("memberId", "1");
    builder_2.set("name", "alice");
    GenericData.Record record_2 = builder_2.build();

    GenericRecordBuilder builder_3 = new GenericRecordBuilder(avroSchemaDiffInNamespace);
    builder_3.set("memberId", "2");
    builder_3.set("name", "bob");
    GenericData.Record record_3 = builder_3.build();

    GenericRecordBuilder builder_4 = new GenericRecordBuilder(nullableSchema);
    builder_4.set("memberId", null);
    builder_4.set("name", "bob");
    GenericData.Record record_4 = builder_4.build();

    GenericRecordBuilder builder_5 = new GenericRecordBuilder(nullableSchema);
    builder_5.set("memberId", null);
    builder_5.set("name", "bob");
    GenericData.Record record_5 = builder_5.build();

    Assert.assertTrue(!record_0.equals(record_1));

    AvroTestTools.GenericRecordWrapper wrapper_0 = new GenericRecordWrapper(record_0);
    GenericRecordWrapper wrapper_1 = new GenericRecordWrapper(record_1);
    GenericRecordWrapper wrapper_2 = new GenericRecordWrapper(record_2);
    GenericRecordWrapper wrapper_3 = new GenericRecordWrapper(record_3);
    GenericRecordWrapper wrapper_4 = new GenericRecordWrapper(record_4);
    GenericRecordWrapper wrapper_5 = new GenericRecordWrapper(record_5);

    Assert.assertEquals(wrapper_0, wrapper_1);
    Assert.assertEquals(wrapper_1, wrapper_2);
    Assert.assertNotSame(wrapper_2, wrapper_3);
    Assert.assertEquals(wrapper_4, wrapper_5);
  }
}