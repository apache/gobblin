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

package org.apache.gobblin.compaction.action;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.iceberg.GobblinMCEProducer;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CompactionGMCEPublishingActionTest {
  @Test
  public void testDedup() throws Exception {

    File basePath = Files.createTempDir();
    basePath.deleteOnExit();

    File jobDir = new File(basePath, "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20");
    Assert.assertTrue(jobDir.mkdirs());

    GenericRecord r1 = createRandomRecord();
    GenericRecord r2 = createRandomRecord();
    GenericRecord r3 = createEvolvedSchemaRecord();
    writeFileWithContent(jobDir, "file1", r1, 20);
    writeFileWithContent(jobDir, "file2", r2, 18);
    File newestFile = writeFileWithContent(jobDir, "file3", r3, 10, r3.getSchema());
    newestFile.setLastModified(Long.MAX_VALUE);

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("dedup", basePath.getAbsolutePath().toString());
    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());
  }

  public GenericRecord createEvolvedSchemaRecord() {
    Schema evolvedSchema = SchemaBuilder.record("evolved")
        .fields()
        .requiredLong("partitionKey")
        .requiredString("environment")
        .requiredString("subKey")
        .optionalString("oppo")
        .endRecord();
    GenericRecordBuilder keyRecordBuilder = new GenericRecordBuilder(evolvedSchema);
    keyRecordBuilder.set("partitionKey", new Long(1));
    keyRecordBuilder.set("environment", "test");
    keyRecordBuilder.set("subKey", "2");
    keyRecordBuilder.set("oppo", "poop");
    return keyRecordBuilder.build();
  }

  // Returning file handler for setting modfication time.
  private File writeFileWithContent(File dir, String fileName, GenericRecord r, int count) throws IOException {
    File file = new File(dir, fileName + "." + count + ".avro");
    Assert.assertTrue(file.createNewFile());
    this.createAvroFileWithRepeatingRecords(file, r, count, Optional.absent());
    return file;
  }

  private File writeFileWithContent(File dir, String fileName, GenericRecord r, int count, Schema schema)
      throws IOException {
    File file = new File(dir, fileName + "." + count + ".avro");
    Assert.assertTrue(file.createNewFile());
    this.createAvroFileWithRepeatingRecords(file, r, count, Optional.of(schema));
    return file;
  }

  public void createAvroFileWithRepeatingRecords(File file, GenericRecord r, int count, Optional<Schema> schema)
      throws IOException {
    DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
    writer.create(schema.isPresent() ? schema.get() : getSchema(), new FileOutputStream(file));
    for (int i = 0; i < count; ++i) {
      writer.append(r);
    }
    writer.close();
  }

  public GenericRecord createRandomRecord() {
    GenericRecordBuilder keyRecordBuilder = new GenericRecordBuilder(getSchema());
    keyRecordBuilder.set("partitionKey", new Long(1));
    keyRecordBuilder.set("environment", "test");
    keyRecordBuilder.set("subKey", "2");
    GenericRecord record = keyRecordBuilder.build();
    return record;
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

  static EmbeddedGobblin createEmbeddedGobblin(String name, String basePath) {
    String pattern = new Path(basePath, "*/*/minutely/*/*/*/*").toString();

    return new EmbeddedGobblin(name).setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY,
        CompactionSource.class.getName())
        .setConfiguration(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern)
        .setConfiguration(MRCompactor.COMPACTION_INPUT_DIR, basePath.toString())
        .setConfiguration(MRCompactor.COMPACTION_INPUT_SUBDIR, "minutely")
        .setConfiguration(MRCompactor.COMPACTION_DEST_DIR, basePath.toString())
        .setConfiguration(MRCompactor.COMPACTION_DEST_SUBDIR, "hourly")
        .setConfiguration(MRCompactor.COMPACTION_TMP_DEST_DIR, "/tmp/compaction/" + name)
        .setConfiguration(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MAX_TIME_AGO, "3000d")
        .setConfiguration(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MIN_TIME_AGO, "1d")
        .setConfiguration(ConfigurationKeys.MAX_TASK_RETRIES_KEY, "0")
        .setConfiguration("compaction.suite.factory",
            "org.apache.gobblin.compaction.suite.CompactionSuiteBaseWithConfigurableCompleteActionFactory")
        .setConfiguration("compaction.complete.actions",
            "org.apache.gobblin.compaction.action.CompactionCompleteFileOperationAction, org.apache.gobblin.compaction.action.CompactionGMCEPublishingAction, org.apache.gobblin.compaction.action.CompactionMarkDirectoryAction")
        .setConfiguration("old.files.hive.registration.policy",
            "org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase")
        .setConfiguration("writer.output.format", "AVRO")
        .setConfiguration(GobblinMCEProducer.GMCE_PRODUCER_CLASS, GobblinMCETestProducer.class.getName())
        .setConfiguration("hive.registration.policy", "com.linkedin.gobblin.hive.policy.LiHiveDailyRegistrationPolicy");
  }

  public static class GobblinMCETestProducer extends GobblinMCEProducer {
    public GobblinMCETestProducer(State state) {
      super(state);
    }

    @Override
    public void underlyingSendGMCE(GobblinMetadataChangeEvent gmce) {
      System.out.println(gmce);
      Assert.assertEquals(gmce.getNewFiles().size(), 1);
      Assert.assertNotEquals(gmce.getOldFilePrefixes().size(), 0);
    }
  }

}
