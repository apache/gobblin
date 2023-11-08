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

package org.apache.gobblin.writer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcFile;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import static org.apache.gobblin.writer.GobblinBaseOrcWriter.CORRUPTED_ORC_FILE_DELETION_EVENT;
import static org.mockito.MockitoAnnotations.openMocks;


public class GobblinBaseOrcWriterTest {
  Configuration conf;
  FileSystem fs;
  File tmpDir;
  File orcFile;
  Path orcFilePath;

  @Mock
  MetricContext mockContext;

  AutoCloseable closeable;

  @BeforeTest
  public void setup() throws IOException {
    this.closeable = openMocks(this);
    this.conf = new Configuration();
    this.fs = FileSystem.getLocal(conf);
    this.tmpDir = Files.createTempDir();
    this.orcFile = new File(tmpDir, "test.orc");
    this.orcFilePath = new Path(orcFile.getAbsolutePath());
  }

  @AfterTest
  public void tearDown()
      throws Exception {
    this.closeable.close();
  }

  @Test
  public void testOrcValidationOnlyHeader()
      throws IOException {
    try (FileWriter writer = new FileWriter(orcFile)) {
      // writer a corrupted ORC file that only contains thethe header
      writer.write(OrcFile.MAGIC);
    }

    Assert.assertThrows(FileFormatException.class, () -> GobblinBaseOrcWriter.assertOrcFileIsValid(
        fs, orcFilePath, new OrcFile.ReaderOptions(conf), mockContext));

    GobblinEventBuilder eventBuilder = new GobblinEventBuilder(CORRUPTED_ORC_FILE_DELETION_EVENT, GobblinBaseOrcWriter.ORC_WRITER_NAMESPACE);
    eventBuilder.addMetadata("filePath", orcFilePath.toString());
    eventBuilder.addMetadata("exceptionType", "org.apache.orc.FileFormatException");
    eventBuilder.addMetadata("exceptionMessage", String.format("Not a valid ORC file %s (maxFileLength= 9223372036854775807)", orcFilePath));
    Mockito.verify(mockContext, Mockito.times(1)).submitEvent(eventBuilder.build());
  }

  @Test
  public void testOrcValidationWithContent() throws IOException {
    try (FileWriter writer = new FileWriter(orcFile)) {
      // write a corrupted ORC file that only contains the header and invalid protobuf content
      writer.write(OrcFile.MAGIC);
      writer.write("\n");
    }

    Assert.assertThrows(com.google.protobuf25.InvalidProtocolBufferException.class,
        () -> GobblinBaseOrcWriter.assertOrcFileIsValid(fs, orcFilePath, new OrcFile.ReaderOptions(conf), mockContext));

    GobblinEventBuilder eventBuilder = new GobblinEventBuilder(CORRUPTED_ORC_FILE_DELETION_EVENT, GobblinBaseOrcWriter.ORC_WRITER_NAMESPACE);
    eventBuilder.addMetadata("filePath", orcFilePath.toString());
    eventBuilder.addMetadata("exceptionType", "com.google.protobuf25.InvalidProtocolBufferException");
    eventBuilder.addMetadata("exceptionMessage", "Protocol message tag had invalid wire type.");
    Mockito.verify(mockContext, Mockito.times(1))
        .submitEvent(eventBuilder.build());
  }
}
