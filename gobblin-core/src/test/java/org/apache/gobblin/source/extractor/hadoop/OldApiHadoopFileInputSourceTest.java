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

package org.apache.gobblin.source.extractor.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Unit tests for {@link OldApiHadoopFileInputSource}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.source.extractor.hadoop"})
public class OldApiHadoopFileInputSourceTest {

  protected static final String TEXT = "This is a test text file";

  protected SourceState sourceState;

  @BeforeClass
  public void setUp() throws IOException {
    File textFile = new File(getFileDir(), "test.txt");
    File dir = textFile.getParentFile();
    if (!dir.exists() && !dir.mkdir()) {
      throw new IOException("Failed to create directory: " + dir);
    }
    if (!textFile.createNewFile()) {
      throw new IOException("Failed to create text file: " + textFile);
    }

    Files.write(TEXT, textFile, ConfigurationKeys.DEFAULT_CHARSET_ENCODING);

    this.sourceState = new SourceState();
    this.sourceState.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, Extract.TableType.SNAPSHOT_ONLY.toString());
    this.sourceState.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "test");
    this.sourceState.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "test");
    this.sourceState.setProp(HadoopFileInputSource.FILE_INPUT_PATHS_KEY, textFile.getAbsolutePath());
  }

  @Test
  public void testGetWorkUnitsAndExtractor() throws IOException, DataRecordException {
    OldApiHadoopFileInputSource<String, Text, LongWritable, Text> fileInputSource = new TestHadoopFileInputSource();

    List<WorkUnit> workUnitList = fileInputSource.getWorkunits(this.sourceState);
    Assert.assertEquals(workUnitList.size(), 1);

    WorkUnitState workUnitState = new WorkUnitState(workUnitList.get(0));

    Closer closer = Closer.create();
    try {
      OldApiHadoopFileInputExtractor<String, Text, LongWritable, Text> extractor =
          (OldApiHadoopFileInputExtractor<String, Text, LongWritable, Text>) fileInputSource.getExtractor(
              workUnitState);
      Text text = extractor.readRecord(null);
      Assert.assertEquals(text.toString(), TEXT);
      Assert.assertNull(extractor.readRecord(null));
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @AfterClass
  public void tearDown() throws IOException {
    File dir = new File(getFileDir());
    FileUtils.deleteDirectory(dir);
  }

  protected String getFileDir() {
    return OldApiHadoopFileInputSourceTest.class.getSimpleName();
  }

  private static class TestHadoopFileInputSource extends OldApiHadoopTextInputSource<String> {

    @Override
    protected OldApiHadoopFileInputExtractor<String, Text, LongWritable, Text> getExtractor(
        WorkUnitState workUnitState, RecordReader<LongWritable, Text> recordReader,
        FileSplit fileSplit, boolean readKeys) {
      return new TestHadoopFileInputExtractor(recordReader, readKeys);
    }
  }

  private static class TestHadoopFileInputExtractor
      extends OldApiHadoopFileInputExtractor<String, Text, LongWritable, Text> {

    public TestHadoopFileInputExtractor(RecordReader<LongWritable, Text> recordReader, boolean readKeys) {
      super(recordReader, readKeys);
    }

    @Override
    public String getSchema() {
      return "";
    }
  }
}
