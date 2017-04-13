package gobblin.compaction.mapreduce;

import com.google.common.io.Files;
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

    String pattern = new Path(basePath.getAbsolutePath().toString(), "*/*/minutely/*/*/*/*").toString();

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("Compaction")
            .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, CompactionSource.class.getName())
            .setConfiguration(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern)
            .setConfiguration(MRCompactor.COMPACTION_INPUT_DIR, basePath.toString())
            .setConfiguration(MRCompactor.COMPACTION_INPUT_SUBDIR, "minutely")
            .setConfiguration(MRCompactor.COMPACTION_DEST_DIR, basePath.toString())
            .setConfiguration(MRCompactor.COMPACTION_DEST_SUBDIR, "hourly")
            .setConfiguration(MRCompactor.COMPACTION_TMP_DEST_DIR, "/tmp/output/test1");

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

    String pattern = new Path(basePath.getAbsolutePath().toString(), "*/*/minutely/*/*/*/*").toString();

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("Compaction")
            .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, CompactionSource.class.getName())
            .setConfiguration(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern)
            .setConfiguration(MRCompactor.COMPACTION_INPUT_DIR, basePath.toString())
            .setConfiguration(MRCompactor.COMPACTION_INPUT_SUBDIR, "minutely")
            .setConfiguration(MRCompactor.COMPACTION_DEST_DIR, basePath.toString())
            .setConfiguration(MRCompactor.COMPACTION_DEST_SUBDIR, "hourly")
            .setConfiguration(MRCompactor.COMPACTION_TMP_DEST_DIR, "/tmp/output/test2")
            .setConfiguration(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, "false");

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

    String pattern = new Path(basePath, "*/*/minutely/*/*/*/*").toString();

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("Compaction")
            .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, CompactionSource.class.getName())
            .setConfiguration(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern)
            .setConfiguration(MRCompactor.COMPACTION_INPUT_DIR, basePath.toString())
            .setConfiguration(MRCompactor.COMPACTION_INPUT_SUBDIR, "minutely")
            .setConfiguration(MRCompactor.COMPACTION_DEST_DIR, basePath.toString())
            .setConfiguration(MRCompactor.COMPACTION_DEST_SUBDIR, "hourly")
            .setConfiguration(MRCompactor.COMPACTION_TMP_DEST_DIR, "/tmp/output/test1");

    JobExecutionResult result = embeddedGobblin.run();
    long recordCount = InputRecordCountHelper.readRecordCount(fs, (new Path (basePath, new Path("Identity/MemberAccount/hourly/2017/04/03/10"))));
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(recordCount, 20);

    // Now write more avro files to input dir
    writeFileWithContent(jobDir, "file2", r1, 22);
    embeddedGobblin.run();
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

}
