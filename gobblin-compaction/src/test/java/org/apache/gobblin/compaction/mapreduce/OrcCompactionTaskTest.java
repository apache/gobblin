package org.apache.gobblin.compaction.mapreduce;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URI;
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
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcStruct;
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

    File jobDir = new File(basePath, "Identity/MemberAccount/minutely/2017/04/03/10/20_30/run_2017-04-03-10-20");
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

    EmbeddedGobblin embeddedGobblin = createEmbeddedGobblin("basic", basePath.getAbsolutePath().toString());
    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());
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

  public void writeOrcFile(Configuration configuration, TypeDescription schema, Path outputPath) throws IOException {
    OrcFile.WriterOptions options = OrcFile.writerOptions(configuration).setSchema(schema);
    Writer writer = OrcFile.createWriter(outputPath, options);
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
