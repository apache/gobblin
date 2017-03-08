package gobblin.mapreduce;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by tamasnemeth on 13/12/16.
 */
public class JsonCompactorOutputFormat extends TextOutputFormat {
  private FileOutputCommitter committer = null;

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    if (this.committer == null) {
      this.committer = new JsonCompactorOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }
    return this.committer;
  }
}