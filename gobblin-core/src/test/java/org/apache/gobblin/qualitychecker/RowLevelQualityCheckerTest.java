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

package org.apache.gobblin.qualitychecker;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyChecker;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyCheckerBuilderFactory;
import java.io.File;
import java.net.URI;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


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

  @Test(groups = {"ignore"})
  public void testWriteToErrFile()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "org.apache.gobblin.qualitychecker.TestRowLevelPolicyFail");
    state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "ERR_FILE");
    state.setProp(ConfigurationKeys.ROW_LEVEL_ERR_FILE, TestConstants.TEST_ERR_FILE);
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
}
