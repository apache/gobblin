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

package org.apache.gobblin.qualitychecker.row;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.TestConstants;
import org.apache.gobblin.qualitychecker.TestRowLevelPolicy;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.records.FlushControlMessageHandler;
import org.apache.gobblin.stream.FlushControlMessage;

import static org.apache.gobblin.configuration.ConfigurationKeys.ROW_LEVEL_ERR_FILE;
import static org.apache.gobblin.qualitychecker.row.RowLevelPolicyChecker.ALLOW_SPECULATIVE_EXECUTION_WITH_ERR_FILE_POLICY;
import static org.apache.gobblin.qualitychecker.row.RowLevelPolicyCheckerBuilder.ROW_LEVEL_POLICY_CHECKER_TYPE;


@Test(groups = {"gobblin.qualitychecker"})
public class RowLevelQualityCheckerTest {
  @Test(groups = {"ignore"})
  public void testRowLevelPolicy()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "org.apache.gobblin.qualitychecker.TestRowLevelPolicy");
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "FAIL");

    RowLevelPolicyChecker checker =
        new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    RowLevelPolicyCheckResults results = new RowLevelPolicyCheckResults();

    FileReader<GenericRecord> fileReader = openFile(state);

    for (GenericRecord datum : fileReader) {
      Assert.assertTrue(checker.executePolicies(datum, results));
    }
  }

  /**
   * Verify close-on-flush for errFile in quality checker.
   */
  public void testErrFileCloseOnFlush() throws Exception {
    File dir = Files.createTempDir();
    dir.deleteOnExit();

    State state = new State();
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "org.apache.gobblin.qualitychecker.TestRowLevelPolicyFail");
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, RowLevelPolicy.Type.ERR_FILE);
    state.setProp(ROW_LEVEL_ERR_FILE, dir.getAbsolutePath());

    RowLevelPolicyChecker checker =
        new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    RowLevelPolicyCheckResults results = new RowLevelPolicyCheckResults();

    Schema intSchema = SchemaBuilder.record("test")
        .fields()
        .name("a").type().intType().noDefault()
        .endRecord();
    GenericRecord intRecord = new GenericRecordBuilder(intSchema)
        .set("a", 1)
        .build();

    GenericRecord intRecord_2 = new GenericRecordBuilder(intSchema)
        .set("a", 2)
        .build();

    Assert.assertFalse(checker.executePolicies(intRecord, results));
    // Inspect the folder: Should see files with zero byte
    FileSystem fs = FileSystem.getLocal(new Configuration());
    List<FileStatus> fileList = Arrays.asList(fs.listStatus(new Path(dir.getPath())));
    Assert.assertEquals(fileList.size(), 1);
    Assert.assertEquals(fileList.get(0).getLen(),0 );

    // This should trigger errFile flush so that file size should be larger than zero.
    checker.getMessageHandler().handleMessage(FlushControlMessage.builder().build());
    fileList = Arrays.asList(fs.listStatus(new Path(dir.getPath())));
    Assert.assertTrue(fileList.get(0).getLen() > 0);

    // This should trigger a new errFile created.
    Assert.assertFalse(checker.executePolicies(intRecord_2, results));
    fileList = Arrays.asList(fs.listStatus(new Path(dir.getPath())));
    Assert.assertEquals(fileList.size(), 2);
  }

  // Verify rowPolicyChecker is configurable.
  public void testRowPolicyCheckerBuilder() throws Exception {
    State state = new State();
    state.setProp(ROW_LEVEL_POLICY_CHECKER_TYPE,
        "org.apache.gobblin.qualitychecker.row.RowLevelQualityCheckerTest$TestRowLevelPolicyChecker");
    RowLevelPolicyChecker checker = RowLevelPolicyCheckerBuilderFactory.newPolicyCheckerBuilder(state, 0).build();
    Assert.assertTrue(checker instanceof TestRowLevelPolicyChecker);
    Assert.assertTrue(checker.getMessageHandler() instanceof FlushControlMessageHandler);
  }

  public void testFileNameWithTimestamp() throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "org.apache.gobblin.qualitychecker.TestRowLevelPolicy");
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "ERR_FILE");
    state.setProp(ROW_LEVEL_ERR_FILE, TestConstants.TEST_ERR_FILE);
    RowLevelPolicyChecker checker =
        new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    Path path = checker.getErrFilePath(new TestRowLevelPolicy(state, RowLevelPolicy.Type.ERR_FILE));

    // Verify that path follows the structure which contains timestamp.
    Pattern pattern = Pattern.compile("test\\/org.apache.gobblin.qualitychecker.TestRowLevelPolicy-\\d+\\.err");
    Matcher matcher = pattern.matcher(path.toString());
    Assert.assertTrue(matcher.matches());

    // Positive case with multiple non-err_file policy specified.
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "org.apache.gobblin.qualitychecker.TestRowLevelPolicy,org.apache.gobblin.qualitychecker.TestRowLevelPolicy");
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "FAIL,OPTIONAL");
    state.setProp(ALLOW_SPECULATIVE_EXECUTION_WITH_ERR_FILE_POLICY, false);
    checker =
        new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    Assert.assertTrue(checker.isSpeculativeAttemptSafe());

    // Negative case with multiple policy containing err_file
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "FAIL,ERR_FILE");
    checker =
        new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    Assert.assertFalse(checker.isSpeculativeAttemptSafe());
  }

  @Test(groups = {"ignore"})
  public void testWriteToErrFile()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "org.apache.gobblin.qualitychecker.TestRowLevelPolicyFail");
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "ERR_FILE");
    state.setProp(ROW_LEVEL_ERR_FILE, TestConstants.TEST_ERR_FILE);
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, TestConstants.TEST_FS_URI);

    RowLevelPolicyChecker checker =
        new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    RowLevelPolicyCheckResults results = new RowLevelPolicyCheckResults();

    FileReader<GenericRecord> fileReader = openFile(state);

    for (GenericRecord datum : fileReader) {
      Assert.assertFalse(checker.executePolicies(datum, results));
    }

    FileSystem fs = FileSystem.get(new URI(TestConstants.TEST_FS_URI), new Configuration());
    Path outputPath = new Path(TestConstants.TEST_ERR_FILE,
        state.getProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST).replaceAll("\\.", "-") + ".err");
    Assert.assertTrue(fs.exists(outputPath));
    fs.delete(new Path(TestConstants.TEST_ERR_FILE), true);
  }

  private FileReader<GenericRecord> openFile(State state)
      throws Exception {
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(new File(TestConstants.TEST_FILE_NAME), reader);
    return fileReader;
  }

  /**
   * An extension of {@link RowLevelPolicyChecker} just for verifying class type when specifying derived class
   * from configuration.
   */
  public static class TestRowLevelPolicyChecker extends RowLevelPolicyChecker {
    public TestRowLevelPolicyChecker(List list, String stateId, FileSystem fs, State state) {
      super(list, stateId, fs, state);
    }

    @Override
    protected ControlMessageHandler getMessageHandler() {
      return new FlushControlMessageHandler(new Flushable() {
        @Override
        public void flush()
            throws IOException {
          // do nothing
        }
      });
    }
  }
}
