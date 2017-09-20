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

package org.apache.gobblin.util;

import com.google.common.base.Optional;
import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.Extract.TableType;

/**
 * Tests for {@link WriterUtils}.
 */
@Test(groups = { "gobblin.util" })
public class WriterUtilsTest {

  public static final Path TEST_WRITER_STAGING_DIR = new Path("gobblin-test/writer-staging/");
  public static final Path TEST_WRITER_OUTPUT_DIR = new Path("gobblin-test/writer-output/");
  public static final Path TEST_WRITER_FILE_PATH = new Path("writer/file/path/");
  public static final Path TEST_DATA_PUBLISHER_FINAL_DIR = new Path("writer/final/dir/");

  @Test
  public void testGetWriterDir() {
    State state = new State();

    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, TEST_WRITER_STAGING_DIR);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, TEST_WRITER_OUTPUT_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getWriterStagingDir(state, 0, 0), new Path(TEST_WRITER_STAGING_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR + ".0", TEST_WRITER_STAGING_DIR);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR + ".0", TEST_WRITER_OUTPUT_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".0", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getWriterStagingDir(state, 2, 0), new Path(TEST_WRITER_STAGING_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR + ".1", TEST_WRITER_STAGING_DIR);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR + ".1", TEST_WRITER_OUTPUT_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".1", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getWriterStagingDir(state, 2, 1), new Path(TEST_WRITER_STAGING_DIR,
        TEST_WRITER_FILE_PATH));
  }

  @Test
  public void testGetDataPublisherFinalOutputDir() {
    State state = new State();

    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, TEST_DATA_PUBLISHER_FINAL_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getDataPublisherFinalDir(state, 0, 0), new Path(TEST_DATA_PUBLISHER_FINAL_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".0", TEST_DATA_PUBLISHER_FINAL_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".0", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getDataPublisherFinalDir(state, 2, 0), new Path(TEST_DATA_PUBLISHER_FINAL_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".1", TEST_DATA_PUBLISHER_FINAL_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".1", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getDataPublisherFinalDir(state, 2, 1), new Path(TEST_DATA_PUBLISHER_FINAL_DIR,
        TEST_WRITER_FILE_PATH));
  }

  @Test
  public void testGetWriterFilePath() {
    Extract extract = new Extract(TableType.SNAPSHOT_ONLY, "org.apache.gobblin.dbNamespace", "tableName");
    WorkUnit state = WorkUnit.create(extract);

    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, TEST_WRITER_FILE_PATH);
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 0, 0), TEST_WRITER_FILE_PATH);

    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".0", TEST_WRITER_FILE_PATH);
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 1, 1), TEST_WRITER_FILE_PATH);

    state.removeProp(ConfigurationKeys.WRITER_FILE_PATH);

    state.setProp(ConfigurationKeys.WRITER_FILE_PATH_TYPE, "tablename");
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 0, 0), new Path("tableName"));

    state.setProp(ConfigurationKeys.WRITER_FILE_PATH_TYPE, "namespace_table");
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 0, 0),
        new Path("org/apache/gobblin/dbNamespace/tableName"));
  }

  @Test
  public void testGetDefaultWriterFilePath() {
    String namespace = "gobblin.test";
    String tableName = "test-table";

    SourceState sourceState = new SourceState();
    WorkUnit state = WorkUnit.create(new Extract(sourceState, TableType.APPEND_ONLY, namespace, tableName));

    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 0, 0), new Path(state.getExtract().getOutputFilePath()));
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 2, 0), new Path(state.getExtract().getOutputFilePath(),
        ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + "0"));
  }

  @Test
  public void testGetDefaultWriterFilePathWithWorkUnitState() {
    String namespace = "gobblin.test";
    String tableName = "test-table";

    SourceState sourceState = new SourceState();
    WorkUnit workUnit = WorkUnit.create(new Extract(sourceState, TableType.APPEND_ONLY, namespace, tableName));
    WorkUnitState workUnitState = new WorkUnitState(workUnit);

    Assert.assertEquals(WriterUtils.getWriterFilePath(workUnitState, 0, 0), new Path(workUnitState.getExtract()
        .getOutputFilePath()));
    Assert.assertEquals(WriterUtils.getWriterFilePath(workUnitState, 2, 0), new Path(workUnitState.getExtract()
        .getOutputFilePath(), ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + "0"));
  }

  @Test
  public void testGetCodecFactoryIgnoresCase() {
    CodecFactory codecFactory = WriterUtils.getCodecFactory(Optional.of("SNAPPY"), Optional.<String>absent());
    Assert.assertEquals(codecFactory.toString(), "snappy");
    codecFactory = WriterUtils.getCodecFactory(Optional.of("snappy"), Optional.<String>absent());
    Assert.assertEquals(codecFactory.toString(), "snappy");
  }
}
