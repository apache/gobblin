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

import com.google.common.io.Files;
import gobblin.compaction.audit.AuditCountClientFactory;
import gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import gobblin.compaction.source.CompactionSource;
import gobblin.data.management.dataset.SimpleDatasetHierarchicalPrioritizer;
import gobblin.compaction.suite.TestCompactionSuiteFactories;
import gobblin.compaction.verify.CompactionAuditCountVerifier;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.compaction.verify.InputRecordCountHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.embedded.EmbeddedGobblin;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MRCompactionTaskTest {

  protected FileSystem getFileSystem()
      throws IOException {
    String uri = ConfigurationKeys.LOCAL_FS_URI;
    FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
    return fs;
  }

  @Test
  public void testDedup() throws Exception {

    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    File jobDir = new File(basePath, "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20");
    Assert.assertTrue(jobDir.mkdirs());

    GenericRecord r1 = createRandomRecord();
    GenericRecord r2 = createRandomRecord();
    writeFileWithContent(jobDir, "file1", r1, 20);
    writeFileWithContent(jobDir, "file2", r2, 18);

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("dedup", basePath.getAbsolutePath().toString());
    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());
  }

  @Test
  public void testNonDedup() throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    File jobDir = new File(basePath, "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20");
    Assert.assertTrue(jobDir.mkdirs());

    GenericRecord r1 = createRandomRecord();
    GenericRecord r2 = createRandomRecord();
    writeFileWithContent(jobDir, "file1", r1, 20);
    writeFileWithContent(jobDir, "file2", r2, 18);

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("non-dedup", basePath.getAbsolutePath().toString());
    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());
  }

  @Test
  public void testRecompaction () throws Exception {
    FileSystem fs = getFileSystem();
    String basePath = "/tmp/testRecompaction";
    fs.delete(new Path(basePath), true);

    File jobDir = new File(basePath, "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20");
    Assert.assertTrue(jobDir.mkdirs());

    GenericRecord r1 = createRandomRecord();
    writeFileWithContent(jobDir, "file1", r1, 20);

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin ("Recompaction-First", basePath);
    JobExecutionResult result = embeddedGobblin.run();
    long recordCount = InputRecordCountHelper.readRecordCount(fs, (new Path (basePath, new Path("Identity/MemberAccount/hourly/2017/04/03/10"))));
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(recordCount, 20);

    // Now write more avro files to input dir
    writeFileWithContent(jobDir, "file2", r1, 22);
    EmbeddedGobblin embeddedGobblin_2 = createEmbeddedGobblin ("Recompaction-Second", basePath);
    embeddedGobblin_2.run();
    Assert.assertTrue(result.isSuccessful());

    // If recompaction is succeeded, a new record count should be written.
    recordCount = InputRecordCountHelper.readRecordCount(fs, (new Path (basePath, new Path("Identity/MemberAccount/hourly/2017/04/03/10"))));
    Assert.assertEquals(recordCount, 42);
    Assert.assertTrue(fs.exists(new Path (basePath, "Identity/MemberAccount/hourly/2017/04/03/10")));
  }

  private void writeFileWithContent(File dir, String fileName, GenericRecord r, int count) throws IOException {
    File file = new File(dir, fileName + "." + count + ".avro");
    Assert.assertTrue(file.createNewFile());
    this.createAvroFileWithRepeatingRecords(file, r, count);
  }

  public Schema getSchema() {
    final String KEY_SCHEMA =
        "{ \"type\" : \"record\",  \"name\" : \"etl\",\"namespace\" : \"reducerTest\",  \"fields\" : [ { \"name\" : "
            + "\"key\", \"type\" : {\"type\" : \"record\", \"name\" : \"key_name\", \"namespace\" : \"key_namespace\",  "
            + "\"fields\" : [ {\"name\" : \"partitionKey\", \"type\" : \"long\", \"doc\" : \"\"}, { \"name\" : \"environment"
            + "\", \"type\" : \"string\",\"doc\" : \"\"}, {\"name\" : \"subKey\",\"type\" : \"string\", \"doc\" : \"\"} ]}, "
            + "\"doc\" : \"\", \"attributes_json\" : \"{\\\"delta\\\":false,\\\"pk\\\":true}\" }]}";
    Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA);
    return keySchema.getField("key").schema();
  }

  public GenericRecord createRandomRecord () {
    GenericRecordBuilder keyRecordBuilder = new GenericRecordBuilder(getSchema());
    keyRecordBuilder.set("partitionKey", new Long(1));
    keyRecordBuilder.set("environment", "test");
    keyRecordBuilder.set("subKey", "2");
    GenericRecord record = keyRecordBuilder.build();
    return record;
  }

  public void createAvroFileWithRepeatingRecords(File file, GenericRecord r, int count) throws IOException {
      DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
      writer.create(getSchema(), new FileOutputStream(file));
      for (int i = 0; i < count; ++i) {
        writer.append(r);
      }
      writer.close();
  }

  private EmbeddedGobblin createEmbeddedGobblin (String name, String basePath) {
    String pattern = new Path(basePath, "*/*/minutely/*/*/*/*").toString();

    return new EmbeddedGobblin(name)
            .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, CompactionSource.class.getName())
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

  private EmbeddedGobblin createEmbeddedGobblinForAllFailures (String name, String basePath) {
    return createEmbeddedGobblin(name, basePath)
        .setConfiguration(AuditCountClientFactory.AUDIT_COUNT_CLIENT_FACTORY, "KafkaAuditCountHttpClientFactory")
        .setConfiguration(CompactionAuditCountVerifier.GOBBLIN_TIER, "dummy")
        .setConfiguration(CompactionAuditCountVerifier.ORIGIN_TIER, "dummy")
        .setConfiguration(CompactionAuditCountVerifier.PRODUCER_TIER, "dummy")
        .setConfiguration(CompactionVerifier.COMPACTION_VERIFICATION_ITERATION_COUNT_LIMIT, "2");
  }

  private EmbeddedGobblin createEmbeddedGobblinForHiveRegistrationFailure (String name, String basePath) {
    return createEmbeddedGobblin(name, basePath)
        .setConfiguration(ConfigurationKeys.COMPACTION_SUITE_FACTORY, "HiveRegistrationFailureFactory");
  }

  private EmbeddedGobblin createEmbeddedGobblinWithPriority (String name, String basePath) {
    return createEmbeddedGobblin(name, basePath)
        .setConfiguration(ConfigurationKeys.COMPACTION_PRIORITIZER_ALIAS, "TieredDatasets")
        .setConfiguration(SimpleDatasetHierarchicalPrioritizer.TIER_KEY + ".0", "Identity")
        .setConfiguration(SimpleDatasetHierarchicalPrioritizer.TIER_KEY + ".1", "EVG")
        .setConfiguration(SimpleDatasetHierarchicalPrioritizer.TIER_KEY + ".2", "BizProfile");
  }

   @Test
   public void testWorkUnitStream () throws Exception {
     File basePath = Files.createTempDir();
     basePath.deleteOnExit();
     GenericRecord r1 = createRandomRecord();
     // verify 24 hours
     for (int i = 22; i < 24; ++i) {
       String path = "Identity/MemberAccount/minutely/2017/04/03/" + i + "/20_30/run_2017-04-03-10-20";
       File jobDir = new File(basePath, path);
       Assert.assertTrue(jobDir.mkdirs());

       writeFileWithContent(jobDir, "file_random", r1, 20);
     }

     EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("workunit_stream", basePath.getAbsolutePath().toString());
     JobExecutionResult result = embeddedGobblin.run();

     Assert.assertTrue(result.isSuccessful());
   }

  @Test
  public void testWorkUnitStreamForAllFailures () throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();
    GenericRecord r1 = createRandomRecord();
    // verify 24 hours
    for (int i = 1; i < 24; ++i) {
      String path = "Identity/MemberAccount/minutely/2017/04/03/" + i + "/20_30/run_2017-04-03-10-20";
      File jobDir = new File(basePath, path);
      Assert.assertTrue(jobDir.mkdirs());

      writeFileWithContent(jobDir, "file_random", r1, 20);
    }

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblinForAllFailures("workunit_stream_all_failure", basePath.getAbsolutePath().toString());
    JobExecutionResult result = embeddedGobblin.run();

    Assert.assertFalse(result.isSuccessful());
  }

  @Test
  public void testHiveRegistrationFailure () throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();
    GenericRecord r1 = createRandomRecord();

    // success dataset
    String path1 = TestCompactionSuiteFactories.DATASET_SUCCESS + "/20_30/run_2017-04-03-10-20";
    File jobDir1 = new File(basePath, path1);
    Assert.assertTrue(jobDir1.mkdirs());
    writeFileWithContent(jobDir1, "file_random", r1, 20);

    // failed dataset
    String path2 = TestCompactionSuiteFactories.DATASET_FAIL + "/20_30/run_2017-04-03-10-20";
    File jobDir2 = new File(basePath, path2);
    Assert.assertTrue(jobDir2.mkdirs());
    writeFileWithContent(jobDir2, "file_random", r1, 20);

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblinForHiveRegistrationFailure("hive_registration_failure", basePath.getAbsolutePath().toString());
    JobExecutionResult result = embeddedGobblin.run();

    Assert.assertFalse(result.isSuccessful());
  }

  @Test
  public void testPrioritization () throws Exception {
    File basePath = Files.createTempDir();
    basePath.deleteOnExit();
    GenericRecord r1 = createRandomRecord();
    // verify 24 hours
    for (int i = 1; i < 3; ++i) {
      String path = "Identity/MemberAccount/minutely/2017/04/03/" + i + "/20_30/run_2017-04-03-10-20";
      File jobDir = new File(basePath, path);
      Assert.assertTrue(jobDir.mkdirs());
      writeFileWithContent(jobDir, "file_random", r1, 20);
    }

    for (int i = 1; i < 3; ++i) {
      String path = "EVG/People/minutely/2017/04/03/" + i + "/20_30/run_2017-04-03-10-20";
      File jobDir = new File(basePath, path);
      Assert.assertTrue(jobDir.mkdirs());
      writeFileWithContent(jobDir, "file_random", r1, 20);
    }

    for (int i = 1; i < 3; ++i) {
      String path = "BizProfile/BizCompany/minutely/2017/04/03/" + i + "/20_30/run_2017-04-03-10-20";
      File jobDir = new File(basePath, path);
      Assert.assertTrue(jobDir.mkdirs());
      writeFileWithContent(jobDir, "file_random", r1, 20);
    }

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblinWithPriority("workunit_stream_priority", basePath.getAbsolutePath().toString());
    JobExecutionResult result = embeddedGobblin.run();

    Assert.assertTrue(result.isSuccessful());
  }
}
