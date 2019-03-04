package org.apache.gobblin.compaction.mapreduce.orc;

import java.io.IOException;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.orc.mapreduce.OrcOutputFormat;


public class OrcKeyCompactorOutputFormat extends OrcOutputFormat {
  private FileOutputCommitter committer = null;

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    return super.getOutputCommitter(context);
  }
}
