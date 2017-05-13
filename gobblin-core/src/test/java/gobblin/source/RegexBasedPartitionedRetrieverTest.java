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
package gobblin.source;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;


public class RegexBasedPartitionedRetrieverTest {
  private Path tempDir;

  private enum DateToUse {
    APR_1_2017(1491004800000L), APR_3_2017(1491177600000L), MAY_1_2017(1493596800000L);

    private final long value;

    DateToUse(long val) {
      this.value = val;
    }

    public long getValue() {
      return value;
    }
  }

  @BeforeClass
  public void setupDirectories()
      throws IOException {
    tempDir = Files.createTempDirectory("regexTest");
    for (DateToUse d : DateToUse.values()) {
      Path subdir = tempDir.resolve(String.format("%d-PT-123456", d.getValue()));
      Files.createDirectory(subdir);
      Files.createFile(subdir.resolve("foo.txt"));
    }
  }

  @AfterClass
  public void cleanup() throws IOException {
    Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc)
          throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Test
  public void testSnapshotRegex() throws IOException {
    String snapshotRegex = "(\\d+)-PT-\\d+";
    RegexBasedPartitionedRetriever r = new RegexBasedPartitionedRetriever("txt");
    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, tempDir.toString());
    state.setProp(PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_PATTERN,
        snapshotRegex);

    r.init(state);

    List<PartitionAwareFileRetriever.FileInfo> files = r.getFilesToProcess(DateToUse.APR_3_2017.getValue() - 1, 9999);
    Assert.assertEquals(files.size(), 2);

    verifyFile(files.get(0), DateToUse.APR_3_2017.getValue());
    verifyFile(files.get(1), DateToUse.MAY_1_2017.getValue());

 }

  private void verifyFile(PartitionAwareFileRetriever.FileInfo fileInfo, long value) {
    org.apache.hadoop.fs.Path expectedStart = new org.apache.hadoop.fs.Path(tempDir.toUri());
    String expectedEnd = String.format("%d-PT-123456/foo.txt", value);

    Assert.assertEquals(fileInfo.getWatermarkMsSinceEpoch(), value);
    Assert.assertTrue(fileInfo.getFilePath().startsWith(expectedStart.toString()));
    Assert.assertTrue(fileInfo.getFilePath().endsWith(expectedEnd));
    Assert.assertEquals(fileInfo.getFileSize(), 0);
   }
}
