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
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;

public class HiveWritableHdfsDataWriterTest {
  private FileSystem fs;
  private File tmpDir;

  @BeforeClass
  public void setUp() throws IOException {
    tmpDir = Files.createTempDir();
    this.fs = FileSystem.get(new Configuration());
  }

  @AfterClass
  public void cleanUp() throws IOException {
    if (this.fs.exists(new Path(this.tmpDir.getAbsolutePath()))) {
      if (!this.fs.delete(new Path(this.tmpDir.getAbsolutePath()), true)) {
        throw new IOException("Failed to clean up path " + this.tmpDir);
      }
    }
  }

  /**
   * Test that multiple close calls do not raise an error
   */
  @Test
  public void testMultipleClose() throws IOException {
    Properties properties = new Properties();
    properties.load(new FileReader("gobblin-core/src/test/resources/writer/hive_writer.properties"));

    properties.setProperty("writer.staging.dir", new Path(tmpDir.getAbsolutePath(), "output-staging").toString());
    properties.setProperty("writer.output.dir", new Path(tmpDir.getAbsolutePath(), "output").toString());
    properties.setProperty("writer.file.path", ".");

    SourceState sourceState = new SourceState(new State(properties), ImmutableList.<WorkUnitState> of());

    DataWriter writer = new HiveWritableHdfsDataWriterBuilder<>().withBranches(1)
            .withWriterId("0").writeTo(Destination.of(Destination.DestinationType.HDFS, sourceState))
            .writeInFormat(WriterOutputFormat.ORC).build();

    writer.close();
    // check for file existence
    Assert.assertTrue(this.fs.exists(new Path(new Path(tmpDir.getAbsolutePath(), "output-staging"), "writer-output.orc")),
        "staging file not found");

    // closed again is okay
    writer.close();
    // commit after close is okay
    writer.commit();
    Assert.assertTrue(this.fs.exists(new Path(new Path(tmpDir.getAbsolutePath(), "output"), "writer-output.orc")),
        "output file not found");
  }
}
