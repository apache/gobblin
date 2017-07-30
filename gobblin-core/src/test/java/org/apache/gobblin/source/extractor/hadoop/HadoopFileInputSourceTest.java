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

package gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.workunit.WorkUnit;


/**
 * Unit tests for {@link HadoopFileInputSource}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.source.extractor.hadoop"})
public class HadoopFileInputSourceTest extends OldApiHadoopFileInputSourceTest {

  @BeforeClass
  public void setUp() throws IOException {
    super.setUp();
  }

  @Test
  public void testGetWorkUnitsAndExtractor() throws IOException, DataRecordException {
    HadoopFileInputSource<String, Text, LongWritable, Text> fileInputSource = new TestHadoopFileInputSource();

    List<WorkUnit> workUnitList = fileInputSource.getWorkunits(this.sourceState);
    Assert.assertEquals(workUnitList.size(), 1);

    WorkUnitState workUnitState = new WorkUnitState(workUnitList.get(0));

    Closer closer = Closer.create();
    try {
      HadoopFileInputExtractor<String, Text, LongWritable, Text> extractor =
          (HadoopFileInputExtractor<String, Text, LongWritable, Text>) fileInputSource.getExtractor(
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
    super.tearDown();
  }

  @Override
  protected String getFileDir() {
    return HadoopFileInputSourceTest.class.getSimpleName();
  }

  private static class TestHadoopFileInputSource extends HadoopTextInputSource<String> {

    @Override
    protected HadoopFileInputExtractor<String, Text, LongWritable, Text> getExtractor(WorkUnitState workUnitState,
        RecordReader<LongWritable, Text> recordReader, FileSplit fileSplit, boolean readKeys) {
      return new TestHadoopFileInputExtractor(recordReader, readKeys);
    }
  }

  private static class TestHadoopFileInputExtractor
      extends HadoopFileInputExtractor<String, Text, LongWritable, Text> {

    public TestHadoopFileInputExtractor(RecordReader<LongWritable, Text> recordReader, boolean readKeys) {
      super(recordReader, readKeys);
    }

    @Override
    public String getSchema() {
      return "";
    }
  }
}
