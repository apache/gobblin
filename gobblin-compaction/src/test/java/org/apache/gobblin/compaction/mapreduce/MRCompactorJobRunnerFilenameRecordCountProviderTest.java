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

package gobblin.compaction.mapreduce;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import gobblin.util.RecordCountProvider;
import gobblin.util.recordcount.IngestionRecordCountProvider;
import gobblin.util.recordcount.LateFileRecordCountProvider;


/**
 * Tests for {@link MRCompactorJobRunner.FileNameFormat}.
 */
@Test(groups = { "gobblin.compaction.mapreduce" })
public class MRCompactorJobRunnerFilenameRecordCountProviderTest {

  @Test
  public void testFileNameRecordCountProvider() throws IOException {
    String originalFilename = "test.123.avro";
    String suffixPattern = Pattern.quote(".late") + "[\\d]*";

    Path testDir = new Path("/tmp/compactorFilenameRecordCountProviderTest");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
      fs.mkdirs(testDir);

      RecordCountProvider originFileNameFormat = new IngestionRecordCountProvider();

      LateFileRecordCountProvider lateFileRecordCountProvider = new LateFileRecordCountProvider(originFileNameFormat);

      Path firstOutput = lateFileRecordCountProvider.constructLateFilePath(originalFilename, fs, testDir);

      Assert.assertEquals(new Path(testDir, originalFilename), firstOutput);
      Assert.assertEquals(123, lateFileRecordCountProvider.getRecordCount(firstOutput));

      fs.create(firstOutput);
      Pattern pattern1 =
          Pattern.compile(Pattern.quote(Files.getNameWithoutExtension(originalFilename)) + suffixPattern + "\\.avro");
      Path secondOutput = lateFileRecordCountProvider.constructLateFilePath(firstOutput.getName(), fs, testDir);
      Assert.assertEquals(testDir, secondOutput.getParent());
      Assert.assertTrue(pattern1.matcher(secondOutput.getName()).matches());
      Assert.assertEquals(123, lateFileRecordCountProvider.getRecordCount(secondOutput));

      fs.create(secondOutput);
      Pattern pattern2 =
          Pattern.compile(Files.getNameWithoutExtension(originalFilename) + suffixPattern + suffixPattern + "\\.avro");
      Path thirdOutput = lateFileRecordCountProvider.constructLateFilePath(secondOutput.getName(), fs, testDir);
      Assert.assertEquals(testDir, thirdOutput.getParent());
      Assert.assertTrue(pattern2.matcher(thirdOutput.getName()).matches());
      Assert.assertEquals(123, lateFileRecordCountProvider.getRecordCount(thirdOutput));
    } finally {
      fs.delete(testDir, true);
    }
  }
}
