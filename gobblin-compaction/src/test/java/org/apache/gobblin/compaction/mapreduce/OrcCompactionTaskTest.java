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

package org.apache.gobblin.compaction.mapreduce;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrcCompactionTaskTest {

  // TODO: Repeated code
  protected FileSystem getFileSystem() throws IOException {
    String uri = ConfigurationKeys.LOCAL_FS_URI;
    FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
    return fs;
  }

  @Test
  public void basicTest() throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    String minutelyPath = "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20";
    String hourlyPath = "Identity/MemberAccount/hourly/2017/04/03/10/";
    File jobDir = new File(basePath, minutelyPath);
    Assert.assertTrue(jobDir.mkdirs());

    // Write some ORC file for compaction here.
    TypeDescription schema = TypeDescription.fromString("struct<i:int,j:int>");
    OrcStruct orcStruct_0 = (OrcStruct) OrcStruct.createValue(schema);
    orcStruct_0.setFieldValue("i", new IntWritable(1));
    orcStruct_0.setFieldValue("j", new IntWritable(2));

    OrcStruct orcStruct_1 = (OrcStruct) OrcStruct.createValue(schema);
    orcStruct_1.setFieldValue("i", new IntWritable(1));
    orcStruct_1.setFieldValue("j", new IntWritable(2));

    File file_0 = new File(jobDir, "file_0");
    File file_1 = new File(jobDir, "file_1");
    writeOrcRecordsInFile(new Path(file_0.getAbsolutePath()), schema, ImmutableList.of(orcStruct_0));
    writeOrcRecordsInFile(new Path(file_1.getAbsolutePath()), schema, ImmutableList.of(orcStruct_1));

    // Verify execution
    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("basic", basePath.getAbsolutePath().toString());
    JobExecutionResult execution = embeddedGobblin.run();
    Assert.assertTrue(execution.isSuccessful());

    // Result verification
    File outputDir = new File(basePath, hourlyPath);
    List<OrcStruct> result = readOrcFile(new Path(outputDir.getAbsolutePath()), "part-r-00000.orc");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getFieldValue("i"), new IntWritable(1));
    Assert.assertEquals(result.get(0).getFieldValue("j"), new IntWritable(2));
  }

  /**
   * Read a output ORC compacted file into memory.
   */
  public List<OrcStruct> readOrcFile(Path orcFileDir, String filename)
      throws IOException, InterruptedException {
    Path orcFilePath = new Path(orcFileDir, filename);
    ReaderImpl orcReader = new ReaderImpl(orcFilePath, new OrcFile.ReaderOptions(new Configuration()));

    Reader.Options options = new Reader.Options().schema(orcReader.getSchema());
    OrcMapreduceRecordReader recordReader = new OrcMapreduceRecordReader(orcReader, options);
    List<OrcStruct> result = new ArrayList<>();

    while (recordReader.nextKeyValue()) {
      result.add((OrcStruct) recordReader.getCurrentValue());
    }

    return result;
  }

  public void writeOrcRecordsInFile(Path path, TypeDescription schema, List<OrcStruct> orcStructs) throws Exception {
    Configuration configuration = new Configuration();
    OrcFile.WriterOptions options = OrcFile.writerOptions(configuration).setSchema(schema);

    Writer writer = OrcFile.createWriter(path, options);
    OrcMapreduceRecordWriter recordWriter = new OrcMapreduceRecordWriter(writer);
    for (OrcStruct orcRecord : orcStructs) {
      recordWriter.write(NullWritable.get(), orcRecord);
    }
    recordWriter.close(new TaskAttemptContextImpl(configuration, new TaskAttemptID()));
  }

  // TODO: Code repeatedness
  private EmbeddedGobblin createEmbeddedGobblin(String name, String basePath) {
    String pattern = new Path(basePath, "*/*/minutely/*/*/*/*").toString();

    return new EmbeddedGobblin(name).setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY,
        CompactionSource.class.getName())
        .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
            CompactionOrcJobConfigurator.Factory.class.getName())
        .setConfiguration(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern)
        .setConfiguration(MRCompactor.COMPACTION_INPUT_DIR, basePath.toString())
        .setConfiguration(MRCompactor.COMPACTION_INPUT_SUBDIR, "minutely")
        .setConfiguration(MRCompactor.COMPACTION_DEST_DIR, basePath.toString())
        .setConfiguration(MRCompactor.COMPACTION_DEST_SUBDIR, "hourly")
        .setConfiguration(MRCompactor.COMPACTION_TMP_DEST_DIR, "/tmp/compaction/" + name)
        .setConfiguration(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MAX_TIME_AGO, "3000d")
        .setConfiguration(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MIN_TIME_AGO, "1d")
        .setConfiguration(ConfigurationKeys.MAX_TASK_RETRIES_KEY, "0");
  }
}
