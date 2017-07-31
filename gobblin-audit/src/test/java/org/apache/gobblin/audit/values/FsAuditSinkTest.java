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
package org.apache.gobblin.audit.values;

import org.apache.gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import org.apache.gobblin.audit.values.sink.FsAuditSink;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;


@Test(groups = { "gobblin.audit.values" })
public class FsAuditSinkTest {

  @Test
  public void testWrite() throws Exception {
    Schema testSchema = SchemaBuilder.record("test").fields().name("f1").type().stringType().noDefault().endRecord();
    GenericRecord r = new GenericRecordBuilder(testSchema).set("f1", "v1").build();
    ValueAuditRuntimeMetadata auditMetadata =
        ValueAuditRuntimeMetadata.builder("db", "tb", testSchema).snapshotId(RandomStringUtils.randomAlphanumeric(5))
            .partFileName("part-1.avro").build();
    File auditFile = null;
    Path auditDir = null;

    try (FsAuditSink auditSink = new FsAuditSink(ConfigFactory.empty(), auditMetadata);) {
      auditFile = new File(auditSink.getAuditFilePath().toString());
      auditDir = auditSink.getAuditDirPath();
      auditSink.write(r);
    } catch (Exception e){
      FileSystem.get(new Configuration()).delete(auditDir, true);
      throw e;
    }

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(testSchema);

    try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(auditFile, reader);) {
      while (dataFileReader.hasNext()) {
        GenericRecord recRead = dataFileReader.next();
        Assert.assertEquals(recRead, r);
        break;
      }
      Assert.assertEquals(dataFileReader.hasNext(), false);
    } finally {
      FileSystem.get(new Configuration()).delete(auditDir, true);
    }
  }
}
