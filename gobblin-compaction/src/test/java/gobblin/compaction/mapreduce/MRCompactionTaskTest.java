package gobblin.compaction.mapreduce;

import com.google.common.io.Files;
import gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import gobblin.compaction.source.CompactionSource;
import gobblin.compaction.verify.InputRecordCountHelper;
import gobblin.configuration.ConfigurationKeys;
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
            .setConfiguration(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MIN_TIME_AGO, "1d");

  }

   @Test
   public void testWorkUnitStream () throws Exception {
     File basePath = Files.createTempDir();
     basePath.deleteOnExit();
     GenericRecord r1 = createRandomRecord();
     // verify 24 hours
     for (int i = 22; i < 25; ++i) {
       String path = "Identity/MemberAccount/minutely/2017/04/03/" + i + "/20_30/run_2017-04-03-10-20";
       File jobDir = new File(basePath, path);
       Assert.assertTrue(jobDir.mkdirs());

       writeFileWithContent(jobDir, "file_random", r1, 20);
     }

     EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("workunit_stream", basePath.getAbsolutePath().toString());
     JobExecutionResult result = embeddedGobblin.run();

     Assert.assertTrue(result.isSuccessful());
   }
}
