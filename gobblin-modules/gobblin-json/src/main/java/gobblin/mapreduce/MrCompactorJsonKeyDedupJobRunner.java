package gobblin.mapreduce;

import com.google.common.collect.Lists;
import gobblin.compaction.dataset.Dataset;
import gobblin.compaction.mapreduce.MRCompactorJobRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by tamasnemeth on 12/11/16.
 */
public class MrCompactorJsonKeyDedupJobRunner extends MRCompactorJobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MrCompactorJsonKeyDedupJobRunner.class);

  public MrCompactorJsonKeyDedupJobRunner(Dataset dataset, FileSystem fs) {
    super(dataset, fs);
    this.dataset.jobProps().setProp("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Override
  protected void configureJob(Job job) throws IOException {
    super.configureJob(job);
    configureCompression(job);
  }

  private void configureCompression(Job job) {
    job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", true);
    job.getConfiguration().set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
  }

  @Override
  protected void setInputFormatClass(Job job) {
    job.setInputFormatClass(TextInputFormat.class);
  }

  @Override
  protected void setMapperClass(Job job) {
    job.setMapperClass(JsonKeyMapper.class);
  }

  @Override
  protected void setMapOutputKeyClass(Job job) {
    job.setMapOutputKeyClass(Text.class);
  }

  @Override
  protected void setMapOutputValueClass(Job job) {
    job.setMapOutputValueClass(Text.class);
  }

  @Override
  protected void setOutputFormatClass(Job job) {
    job.setOutputFormatClass(JsonCompactorOutputFormat.class);
  }

  @Override
  protected void setReducerClass(Job job) {
    job.setReducerClass(JsonKeyDedupReducer.class);
  }

  @Override
  protected void setOutputKeyClass(Job job) {
    job.setOutputKeyClass(Text.class);
  }

  @Override
  protected void setOutputValueClass(Job job) {
    job.setOutputValueClass(NullWritable.class);
  }

  @Override
  protected Collection<String> getApplicableFileExtensions() {
    return Lists.newArrayList("gz");
  }


}
