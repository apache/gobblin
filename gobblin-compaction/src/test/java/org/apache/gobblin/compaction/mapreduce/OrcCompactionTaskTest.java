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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import org.apache.gobblin.compaction.mapreduce.orc.OrcTestUtils;
import org.apache.gobblin.compaction.mapreduce.orc.OrcUtils;
import org.apache.gobblin.compaction.mapreduce.test.TestCompactionOrcJobConfigurator;
import org.apache.gobblin.compaction.mapreduce.test.TestCompactionTaskUtils;
import org.apache.gobblin.compaction.verify.InputRecordCountHelper;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;

import static org.apache.gobblin.compaction.mapreduce.CompactionCombineFileInputFormat.COMPACTION_JOB_MAPRED_MAX_SPLIT_SIZE;
import static org.apache.gobblin.compaction.mapreduce.CompactionCombineFileInputFormat.COMPACTION_JOB_MAPRED_MIN_SPLIT_SIZE;
import static org.apache.gobblin.compaction.mapreduce.CompactionOrcJobConfigurator.ORC_MAPPER_SHUFFLE_KEY_SCHEMA;
import static org.apache.gobblin.compaction.mapreduce.CompactorOutputCommitter.COMPACTION_OUTPUT_EXTENSION;
import static org.apache.gobblin.compaction.mapreduce.MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET;
import static org.apache.gobblin.compaction.mapreduce.MRCompactor.COMPACTION_SHOULD_DEDUPLICATE;
import static org.apache.gobblin.compaction.mapreduce.test.TestCompactionTaskUtils.createEmbeddedGobblinCompactionJob;

@Test(groups = {"gobblin.compaction"})
public class OrcCompactionTaskTest {
  final String extensionName = "orc";

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

    // Following pattern: FILENAME.RECORDCOUNT.EXTENSION
    File file_0 = new File(jobDir, "file_0.2." + extensionName);
    File file_1 = new File(jobDir, "file_1.2." + extensionName);

    writeOrcRecordsInFile(new Path(file_0.getAbsolutePath()), schema, ImmutableList.of(orcStruct_0, orcStruct_2));
    writeOrcRecordsInFile(new Path(file_1.getAbsolutePath()), schema, ImmutableList.of(orcStruct_1, orcStruct_3));
  }

  /**
   * This test case covers the scenarios when the split size is smaller than an actual file size:
   * RecordReader should stop on the boundary when read records that will make the split beyond the max_split_size,
   * and the subsequent reader should just load the remaining records.
   */
  @Test
  public void testWithPartialFileInSplit() throws Exception {
    File basePath = Files.createTempDir();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    basePath.deleteOnExit();

    String minutelyPath = "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20";
    String hourlyPath = "Identity/MemberAccount/hourly/2017/04/03/10/";
    File jobDir = new File(basePath, minutelyPath);
    Assert.assertTrue(jobDir.mkdirs());

    // Writing some basic ORC files
    // Testing data is schema'ed with "struct<i:int,j:int>"
    createTestingData(jobDir);

    // sample a file size
    FileStatus[] statuses = fs.listStatus(new Path(jobDir.getAbsolutePath()));
    Assert.assertTrue(statuses.length > 0 );
    long splitSize = statuses[0].getLen() / 2 ;
    Assert.assertTrue(splitSize > 0);

    EmbeddedGobblin embeddedGobblin = TestCompactionTaskUtils.createEmbeddedGobblinCompactionJob("basic", basePath.getAbsolutePath())
        .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
            TestCompactionOrcJobConfigurator.Factory.class.getName())
        // Each file generated by the data-creation function is around 250 bytes in terms of length.
        // Setting the max split size to be half the size force a single file to be split.
        .setConfiguration(COMPACTION_JOB_MAPRED_MAX_SPLIT_SIZE, splitSize + "")
        .setConfiguration(COMPACTION_JOB_MAPRED_MIN_SPLIT_SIZE, splitSize + "")
        .setConfiguration(COMPACTION_OUTPUT_EXTENSION, extensionName);
    JobExecutionResult execution = embeddedGobblin.run();
    Assert.assertTrue(execution.isSuccessful());

    // Result verification: Verify the duplicate count is expected.
    File outputDir = new File(basePath, hourlyPath);
    State state = new State();
    state.setProp(COMPACTION_OUTPUT_EXTENSION, "orc");
    InputRecordCountHelper stateHelper = new InputRecordCountHelper(state);
    Assert.assertEquals(stateHelper.readRecordCount(new Path(outputDir.getAbsolutePath())), 4);
    Assert.assertEquals(stateHelper.readDuplicationCount(new Path(outputDir.getAbsolutePath())), 1);
  }

  @Test
  public void basicTestWithShuffleKeySpecified() throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    String minutelyPath = "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20";
    String hourlyPath = "Identity/MemberAccount/hourly/2017/04/03/10/";
    File jobDir = new File(basePath, minutelyPath);
    Assert.assertTrue(jobDir.mkdirs());

    // Writing some basic ORC files
    // Testing data is schema'ed with "struct<i:int,j:int>"
    createTestingData(jobDir);

    EmbeddedGobblin embeddedGobblin = TestCompactionTaskUtils.createEmbeddedGobblinCompactionJob("basic", basePath.getAbsolutePath())
        .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
            TestCompactionOrcJobConfigurator.Factory.class.getName())
        .setConfiguration(COMPACTION_OUTPUT_EXTENSION, extensionName)
        // A shuffle key that shouldn't be taken.
        .setConfiguration(ORC_MAPPER_SHUFFLE_KEY_SCHEMA, "struct<k:int>");
    JobExecutionResult execution = embeddedGobblin.run();
    Assert.assertTrue(execution.isSuccessful());

    // Result verification
    File outputDir = new File(basePath, hourlyPath);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    List<FileStatus> statuses = new ArrayList<>();
    reloadFolder(statuses, outputDir, fs);

    Assert.assertTrue(statuses.size() == 1);
    List<OrcStruct> result = readOrcFile(statuses.get(0).getPath());
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0).getFieldValue("i"), new IntWritable(1));
    Assert.assertEquals(result.get(0).getFieldValue("j"), new IntWritable(2));
    Assert.assertEquals(result.get(1).getFieldValue("i"), new IntWritable(2));
    Assert.assertEquals(result.get(1).getFieldValue("j"), new IntWritable(3));
    Assert.assertEquals(result.get(2).getFieldValue("i"), new IntWritable(4));
    Assert.assertEquals(result.get(2).getFieldValue("j"), new IntWritable(5));
  }

  @Test
  public void basicTestWithRecompactionAndBasicSchemaEvolution() throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    String minutelyPath = "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20";
    String hourlyPath = "Identity/MemberAccount/hourly/2017/04/03/10/";
    File jobDir = new File(basePath, minutelyPath);
    Assert.assertTrue(jobDir.mkdirs());

    // Writing some basic ORC files
    createTestingData(jobDir);

    // Writing an additional file with ** evolved schema **.
    TypeDescription evolvedSchema = TypeDescription.fromString("struct<i:int,j:int,k:int>");
    OrcStruct orcStruct_4 = (OrcStruct) OrcStruct.createValue(evolvedSchema);
    orcStruct_4.setFieldValue("i", new IntWritable(5));
    orcStruct_4.setFieldValue("j", new IntWritable(6));
    orcStruct_4.setFieldValue("k", new IntWritable(7));

    File file_2 = new File(jobDir, "file_2.1." + extensionName);
    writeOrcRecordsInFile(new Path(file_2.getAbsolutePath()), evolvedSchema, ImmutableList.of(orcStruct_4));
    // Make this is the newest.
    file_2.setLastModified(Long.MAX_VALUE);

    // Verify execution
    // Overwrite the job configurator factory key.
    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblinCompactionJob("basic", basePath.getAbsolutePath())
        .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
        TestCompactionOrcJobConfigurator.Factory.class.getName())
        .setConfiguration(COMPACTION_OUTPUT_EXTENSION, extensionName)
        .setConfiguration(COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET, "Identity.*:0.1");
    JobExecutionResult execution = embeddedGobblin.run();
    Assert.assertTrue(execution.isSuccessful());

    // Result verification
    File outputDir = new File(basePath, hourlyPath);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    List<FileStatus> statuses = new ArrayList<>();
    reloadFolder(statuses, outputDir, fs);

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

    // Adding new .orc file into the directory and verify if re-compaction is triggered.
    File file_late = new File(jobDir, "file_late.1." + extensionName);
    OrcStruct orcStruct_5 = (OrcStruct) OrcStruct.createValue(evolvedSchema);
    orcStruct_5.setFieldValue("i", new IntWritable(10));
    orcStruct_5.setFieldValue("j", new IntWritable(11));
    orcStruct_5.setFieldValue("k", new IntWritable(12));

    writeOrcRecordsInFile(new Path(file_late.getAbsolutePath()), evolvedSchema, ImmutableList.of(orcStruct_5));
    execution = embeddedGobblin.run();
    Assert.assertTrue(execution.isSuccessful());

    reloadFolder(statuses, outputDir, fs);
    result = readOrcFile(statuses.get(0).getPath());
    // Note previous execution's inspection gives 4 result, given re-compaction, this should gives 1 late-record more.
    Assert.assertEquals(result.size(), 4 + 1);
  }

  @Test
  public void testReducerSideDedup() throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    String minutelyPath = "Identity/MemberAccount/minutely/2020/04/03/10/20_30/run_2020-04-03-10-20";
    String hourlyPath = "Identity/MemberAccount/hourly/2020/04/03/10/";
    File jobDir = new File(basePath, minutelyPath);
    Assert.assertTrue(jobDir.mkdirs());

    TypeDescription nestedSchema = TypeDescription.fromString("struct<a:struct<a:int,b:string,c:int>,b:string,c:uniontype<int,string>>");
    // Create three records with same value except "b" column in the top-level.
    OrcStruct nested_struct_1 = (OrcStruct) OrcUtils.createValueRecursively(nestedSchema);
    OrcTestUtils.fillOrcStructWithFixedValue(nested_struct_1, nestedSchema, 1, "test1", true);
    ((OrcStruct)nested_struct_1).setFieldValue("b", new Text("uno"));
    OrcStruct nested_struct_2 = (OrcStruct) OrcUtils.createValueRecursively(nestedSchema);
    OrcTestUtils.fillOrcStructWithFixedValue(nested_struct_2, nestedSchema, 1, "test2", true);
    ((OrcStruct)nested_struct_2).setFieldValue("b", new Text("dos"));
    OrcStruct nested_struct_3 = (OrcStruct) OrcUtils.createValueRecursively(nestedSchema);
    OrcTestUtils.fillOrcStructWithFixedValue(nested_struct_3, nestedSchema, 1, "test3", true);
    ((OrcStruct)nested_struct_3).setFieldValue("b", new Text("tres"));
    // Create another two records with different value from the above three, and these two differs in column b as well.
    OrcStruct nested_struct_4 = (OrcStruct) OrcUtils.createValueRecursively(nestedSchema);
    OrcTestUtils.fillOrcStructWithFixedValue(nested_struct_4, nestedSchema, 2, "test2", false);
    ((OrcStruct)nested_struct_4).setFieldValue("b", new Text("uno"));
    // This record will be considered as a duplication as nested_struct_4
    OrcStruct nested_struct_5 = (OrcStruct) OrcUtils.createValueRecursively(nestedSchema);
    OrcTestUtils.fillOrcStructWithFixedValue(nested_struct_5, nestedSchema, 2, "test2", false);
    ((OrcStruct)nested_struct_5).setFieldValue("b", new Text("uno"));

    // Following pattern: FILENAME.RECORDCOUNT.EXTENSION
    File file_0 = new File(jobDir, "file_0.5." + extensionName);
    writeOrcRecordsInFile(new Path(file_0.getAbsolutePath()), nestedSchema, ImmutableList.of(nested_struct_1,
        nested_struct_2, nested_struct_3, nested_struct_4, nested_struct_5));

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblinCompactionJob("basic", basePath.getAbsolutePath().toString())
        .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
            TestCompactionOrcJobConfigurator.Factory.class.getName())
        .setConfiguration(COMPACTION_OUTPUT_EXTENSION, extensionName)
        .setConfiguration(ORC_MAPPER_SHUFFLE_KEY_SCHEMA, "struct<a:struct<a:int,c:int>>");
    JobExecutionResult execution = embeddedGobblin.run();
    Assert.assertTrue(execution.isSuccessful());

    // Verifying result: Reducer should catch all the false-duplicates
    File outputDir = new File(basePath, hourlyPath);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    List<FileStatus> statuses = new ArrayList<>();
    reloadFolder(statuses, outputDir, fs);
    Assert.assertEquals(statuses.size(), 1);
    List<OrcStruct> result = readOrcFile(statuses.get(0).getPath());
    // Should still contain original 3 records since they have different value in columns not included in shuffle key.
    Assert.assertEquals(result.size(), 4);
    Assert.assertTrue(result.contains(nested_struct_1));
    Assert.assertTrue(result.contains(nested_struct_2));
    Assert.assertTrue(result.contains(nested_struct_3));
    Assert.assertTrue(result.contains(nested_struct_4));
  }

  // A helper method to load all files in the output directory for compaction-result inspection.
  private void reloadFolder(List<FileStatus> statuses, File outputDir, FileSystem fs) throws IOException {
    statuses.clear();
    for (FileStatus status : fs.listStatus(new Path(outputDir.getAbsolutePath()), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return FilenameUtils.isExtension(path.getName(), extensionName);
      }
    })) {
      statuses.add(status);
    }
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

    EmbeddedGobblin embeddedGobblin_nondedup = createEmbeddedGobblinCompactionJob("basic", basePath.getAbsolutePath().toString())
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
   * This only works if fields are int value.
   */
  private List<OrcStruct> readOrcFile(Path orcFilePath)
      throws IOException, InterruptedException {
    ReaderImpl orcReader = new ReaderImpl(orcFilePath, new OrcFile.ReaderOptions(new Configuration()));

    Reader.Options options = new Reader.Options().schema(orcReader.getSchema());
    OrcMapreduceRecordReader recordReader = new OrcMapreduceRecordReader(orcReader, options);
    List<OrcStruct> result = new ArrayList<>();

    OrcStruct recordContainer;
    while (recordReader.nextKeyValue()) {
      recordContainer = (OrcStruct) OrcUtils.createValueRecursively(orcReader.getSchema());
      OrcUtils.upConvertOrcStruct((OrcStruct) recordReader.getCurrentValue(), recordContainer, orcReader.getSchema());
      result.add(recordContainer);
    }

    return result;
  }

  private void writeOrcRecordsInFile(Path path, TypeDescription schema, List<OrcStruct> orcStructs) throws Exception {
    Configuration configuration = new Configuration();
    OrcFile.WriterOptions options = OrcFile.writerOptions(configuration).setSchema(schema);

    Writer writer = OrcFile.createWriter(path, options);
    OrcMapreduceRecordWriter recordWriter = new OrcMapreduceRecordWriter(writer);
    for (OrcStruct orcRecord : orcStructs) {
      recordWriter.write(NullWritable.get(), orcRecord);
    }
    recordWriter.close(new TaskAttemptContextImpl(configuration, new TaskAttemptID()));
  }
}
