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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
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

import static org.apache.gobblin.compaction.mapreduce.AvroCompactionTaskTest.*;
import static org.apache.gobblin.compaction.mapreduce.CompactorOutputCommitter.*;
import static org.apache.gobblin.compaction.mapreduce.MRCompactor.COMPACTION_SHOULD_DEDUPLICATE;


public class OrcCompactionTaskTest {
  private void createTestingData(File jobDir) throws Exception {
    // Write some ORC file for compaction here.
    TypeDescription schema = TypeDescription.fromString("struct<i:int,j:int>");
    OrcStruct orcStruct_0 = (OrcStruct) OrcStruct.createValue(schema);
    orcStruct_0.setFieldValue("i", new IntWritable(1));
    orcStruct_0.setFieldValue("j", new IntWritable(2));

    OrcStruct orcStruct_1 = (OrcStruct) OrcStruct.createValue(schema);
    orcStruct_1.setFieldValue("i", new IntWritable(1));
    orcStruct_1.setFieldValue("j", new IntWritable(2));

    OrcStruct orcStruct_2 = (OrcStruct) OrcStruct.createValue(schema);
    orcStruct_2.setFieldValue("i", new IntWritable(2));
    orcStruct_2.setFieldValue("j", new IntWritable(3));

    OrcStruct orcStruct_3 = (OrcStruct) OrcStruct.createValue(schema);
    orcStruct_3.setFieldValue("i", new IntWritable(4));
    orcStruct_3.setFieldValue("j", new IntWritable(5));

    File file_0 = new File(jobDir, "file_0");
    File file_1 = new File(jobDir, "file_1");

    writeOrcRecordsInFile(new Path(file_0.getAbsolutePath()), schema, ImmutableList.of(orcStruct_0, orcStruct_2));
    writeOrcRecordsInFile(new Path(file_1.getAbsolutePath()), schema, ImmutableList.of(orcStruct_1, orcStruct_3));
  }

  @Test
  public void basicTest() throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    String minutelyPath = "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20";
    String hourlyPath = "Identity/MemberAccount/hourly/2017/04/03/10/";
    File jobDir = new File(basePath, minutelyPath);
    Assert.assertTrue(jobDir.mkdirs());

    // Writing some basic ORC files
    createTestingData(jobDir);

    // Writing an additional file with evolved schema.
    TypeDescription evolvedSchema = TypeDescription.fromString("struct<i:int,j:int,k:int>");
    OrcStruct orcStruct_4 = (OrcStruct) OrcStruct.createValue(evolvedSchema);
    orcStruct_4.setFieldValue("i", new IntWritable(5));
    orcStruct_4.setFieldValue("j", new IntWritable(6));
    orcStruct_4.setFieldValue("k", new IntWritable(7));

    File file_2 = new File(jobDir, "file_2");
    writeOrcRecordsInFile(new Path(file_2.getAbsolutePath()), evolvedSchema, ImmutableList.of(orcStruct_4));
    // Make this is the newest.
    file_2.setLastModified(Long.MAX_VALUE);

    // Verify execution
    // Overwrite the job configurator factory key.
    String extensionFileName = "orcavro";
    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("basic", basePath.getAbsolutePath().toString())
        .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
        TestCompactionOrcJobConfigurator.Factory.class.getName())
        .setConfiguration(COMPACTION_OUTPUT_EXTENSION, extensionFileName);
    JobExecutionResult execution = embeddedGobblin.run();
    Assert.assertTrue(execution.isSuccessful());

    // Result verification
    File outputDir = new File(basePath, hourlyPath);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    List<FileStatus> statuses = new ArrayList<>();
    for (FileStatus status : fs.listStatus(new Path(outputDir.getAbsolutePath()), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return FilenameUtils.isExtension(path.getName(), extensionFileName);
      }
    })) {
      statuses.add(status);
    }

    Assert.assertTrue(statuses.size() == 1);
    List<OrcStruct> result = readOrcFile(statuses.get(0).getPath());
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get(0).getFieldValue("i"), new IntWritable(1));
    Assert.assertEquals(result.get(0).getFieldValue("j"), new IntWritable(2));
    Assert.assertNull(result.get(0).getFieldValue("k"));
    Assert.assertEquals(result.get(1).getFieldValue("i"), new IntWritable(2));
    Assert.assertEquals(result.get(1).getFieldValue("j"), new IntWritable(3));
    Assert.assertNull(result.get(1).getFieldValue("k"));
    Assert.assertEquals(result.get(2).getFieldValue("i"), new IntWritable(4));
    Assert.assertEquals(result.get(2).getFieldValue("j"), new IntWritable(5));
    Assert.assertNull(result.get(2).getFieldValue("k"));
    Assert.assertEquals(result.get(3).getFieldValue("i"), new IntWritable(5));
    Assert.assertEquals(result.get(3).getFieldValue("j"), new IntWritable(6));
    Assert.assertEquals(result.get(3).getFieldValue("k"), new IntWritable(7));
  }

  @Test
  public void testNonDedup() throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    String minutelyPath = "Identity/MemberAccount_2/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20";
    String hourlyPath = "Identity/MemberAccount_2/hourly/2017/04/03/10/";
    File jobDir = new File(basePath, minutelyPath);
    Assert.assertTrue(jobDir.mkdirs());

    createTestingData(jobDir);

    EmbeddedGobblin embeddedGobblin_nondedup = createEmbeddedGobblin("basic", basePath.getAbsolutePath().toString())
        .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
            TestCompactionOrcJobConfigurator.Factory.class.getName())
        .setConfiguration(COMPACTION_OUTPUT_EXTENSION, "orc")
        .setConfiguration(COMPACTION_SHOULD_DEDUPLICATE, "false");
    JobExecutionResult execution = embeddedGobblin_nondedup.run();
    Assert.assertTrue(execution.isSuccessful());

    // Non-dedup result verification
    File outputDir = new File(basePath, hourlyPath);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    List<FileStatus> statuses = new ArrayList<>();
    for (FileStatus status : fs.listStatus(new Path(outputDir.getAbsolutePath()), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return FilenameUtils.isExtension(path.getName(), "orc");
      }
    })) {
      statuses.add(status);
    }

    Assert.assertTrue(statuses.size() == 1);
    List<OrcStruct> result = readOrcFile(statuses.get(0).getPath());
    Assert.assertEquals(result.size(), 4);

    result.sort(new Comparator<OrcStruct>() {
      @Override
      public int compare(OrcStruct o1, OrcStruct o2) {
        return o1.compareTo(o2);
      }
    });
    Assert.assertEquals(result.get(0).getFieldValue("i"), new IntWritable(1));
    Assert.assertEquals(result.get(0).getFieldValue("j"), new IntWritable(2));
    Assert.assertEquals(result.get(1).getFieldValue("i"), new IntWritable(1));
    Assert.assertEquals(result.get(1).getFieldValue("j"), new IntWritable(2));
    Assert.assertEquals(result.get(2).getFieldValue("i"), new IntWritable(2));
    Assert.assertEquals(result.get(2).getFieldValue("j"), new IntWritable(3));
    Assert.assertEquals(result.get(3).getFieldValue("i"), new IntWritable(4));
    Assert.assertEquals(result.get(3).getFieldValue("j"), new IntWritable(5));
  }

  /**
   * Read a output ORC compacted file into memory.
   */
  public List<OrcStruct> readOrcFile(Path orcFilePath)
      throws IOException, InterruptedException {
    ReaderImpl orcReader = new ReaderImpl(orcFilePath, new OrcFile.ReaderOptions(new Configuration()));

    Reader.Options options = new Reader.Options().schema(orcReader.getSchema());
    OrcMapreduceRecordReader recordReader = new OrcMapreduceRecordReader(orcReader, options);
    List<OrcStruct> result = new ArrayList<>();

    while (recordReader.nextKeyValue()) {
      result.add(copyIntOrcStruct((OrcStruct) recordReader.getCurrentValue()));
    }

    return result;
  }

  private OrcStruct copyIntOrcStruct(OrcStruct record) {
    OrcStruct result = new OrcStruct(record.getSchema());
    for (int i = 0 ; i < record.getNumFields() ; i ++ ) {
      if (record.getFieldValue(i) != null) {
        IntWritable newCopy = new IntWritable(((IntWritable) record.getFieldValue(i)).get());
        result.setFieldValue(i, newCopy);
      } else {
        result.setFieldValue(i, null);
      }
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

  private static class TestCompactionOrcJobConfigurator extends CompactionOrcJobConfigurator {
    public static class Factory implements CompactionJobConfigurator.ConfiguratorFactory {
      @Override
      public TestCompactionOrcJobConfigurator createConfigurator(State state) throws IOException {
        return new TestCompactionOrcJobConfigurator(state);
      }
    }

    @Override
    protected void setNumberOfReducers(Job job) throws IOException {
      job.setNumReduceTasks(1);
    }

    public TestCompactionOrcJobConfigurator(State state) throws IOException {
      super(state);
    }
  }
}
