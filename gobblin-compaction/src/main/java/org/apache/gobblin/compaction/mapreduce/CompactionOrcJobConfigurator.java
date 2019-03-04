package org.apache.gobblin.compaction.mapreduce;

import java.io.IOException;
import org.apache.gobblin.compaction.mapreduce.orc.OrcKeyComparator;
import org.apache.gobblin.compaction.mapreduce.orc.OrcKeyDedupReducer;
import org.apache.gobblin.compaction.mapreduce.orc.OrcUtils;
import org.apache.gobblin.compaction.mapreduce.orc.OrcValueMapper;
import org.apache.gobblin.compaction.mapreduce.orc.OrcValueRecursiveCombineFileInputFormat;
import org.apache.gobblin.configuration.State;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcOutputFormat;


public class CompactionOrcJobConfigurator extends CompactionJobConfigurator {
  public static class Factory implements CompactionJobConfigurator.ConfiguratorFactory {
    @Override
    public CompactionJobConfigurator createConfigurator(State state) throws IOException {
      return new CompactionOrcJobConfigurator(state);
    }
  }

  public CompactionOrcJobConfigurator(State state) throws IOException {
    super(state);
  }

  protected void configureSchema(Job job) throws IOException {
    TypeDescription schema = OrcUtils.getNewestSchemaFromSource(job, this.fs);

    job.getConfiguration().set(OrcConf.MAPRED_INPUT_SCHEMA.getAttribute(), schema.toString());
    job.getConfiguration().set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), schema.toString());
    job.getConfiguration().set(OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.getAttribute(), schema.toString());
    job.getConfiguration().set(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute(), schema.toString());
  }

  protected void configureMapper(Job job) {
    job.setInputFormatClass(OrcValueRecursiveCombineFileInputFormat.class);
    job.setMapperClass(OrcValueMapper.class);
    job.setMapOutputKeyClass(OrcKey.class);
    job.setMapOutputValueClass(OrcValue.class);
    job.setGroupingComparatorClass(OrcKeyComparator.class);
    job.setSortComparatorClass(OrcKeyComparator.class);
  }

  protected void configureReducer(Job job) {
    job.setReducerClass(OrcKeyDedupReducer.class);
    job.setOutputFormatClass(OrcOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcValue.class);
    // TODO: Hardcoded for now.
    job.setNumReduceTasks(2);
  }
}
