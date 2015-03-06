package gobblin.runtime.mapreduce;

import gobblin.configuration.ConfigurationKeys;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

/**
 * Hadoop {@link org.apache.hadoop.mapreduce.OutputFormat} implementation with a custom {@link OutputCommitter}
 */
public class GobblinOutputFormat extends NullOutputFormat<NullWritable, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(GobblinOutputFormat.class);

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new GobblinOutputCommitter();
  }

  /**
   * Hadoop {@link OutputCommitter} implementation that overrides the default
   * {@link #abortJob(JobContext, org.apache.hadoop.mapreduce.JobStatus.State)} behavior. This is necessary to add
   * functionality for cleaning up staging data when the
   * {@link com.linkedin.uif.runtime.JobLauncher#cancelJob(Properties)} method is called via Azkaban. Azkaban only
   * allows the cancel method run to for 5 ms until it does a hard kill on the process, so instead, we will clean up the
   * staging data in the AM.
   */
  private class GobblinOutputCommitter extends OutputCommitter {

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
      LOG.info("Aborting Job: " + jobContext.getJobID());

      Configuration conf = jobContext.getConfiguration();

      URI fsUri = URI.create(conf.get(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
      FileSystem fs = FileSystem.get(fsUri, conf);

      Path mrJobDir =
          new Path(conf.get(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY), conf.get(ConfigurationKeys.JOB_NAME_KEY));
      Path jobInputDir = new Path(mrJobDir, "input");

      for (FileStatus status : fs.listStatus(jobInputDir)) {

        Closer workUnitFileCloser = Closer.create();
        if (status.getPath().getName().endsWith(".wu")) {

          WorkUnit wu = new WorkUnit();
          wu.readFields(workUnitFileCloser.register(new DataInputStream(fs.open(status.getPath()))));
          workUnitFileCloser.close();

          LOG.info("Cleaning up staging data for WorkUnit: " + wu.getId());
          int numBranches = wu.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
          for (int i = 0; i < numBranches; i++) {

            String branchName = ForkOperatorUtils.getBranchName(wu, i, ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + i);

            Path writerFilePath =
                new Path(ForkOperatorUtils.getPathForBranch(wu.getExtract().getOutputFilePath(), branchName,
                    numBranches));

            String uri =
                wu.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, i),
                    ConfigurationKeys.LOCAL_FS_URI);

            FileSystem wufs = FileSystem.get(URI.create(uri), new Configuration());

            if (wu.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR,
                numBranches, i))) {
              Path stagingDir =
                  new Path(wu.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR,
                      i)), writerFilePath);

              LOG.info("Deleting staging dir:   " + stagingDir);
              if (wufs.exists(stagingDir)) {
                wufs.delete(stagingDir, true);
              }
            }

            if (wu.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR,
                numBranches, i))) {
              Path outputDir =
                  new Path(wu.getProp(ForkOperatorUtils
                      .getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, i)), writerFilePath);

              LOG.info("Deleting output dir:    " + outputDir);
              if (wufs.exists(outputDir)) {
                wufs.delete(outputDir, true);
              }
            }
          }
        }
        // TODO add support for MultiWorkUnits
      }
      fs.delete(mrJobDir, true);
    }

    @Override
    public void abortTask(TaskAttemptContext arg0) throws IOException { }

    @Override
    public void commitTask(TaskAttemptContext arg0) throws IOException { }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
      return false;
    }

    @Override
    public void setupJob(JobContext arg0) throws IOException { }

    @Override
    public void setupTask(TaskAttemptContext arg0) throws IOException { }

    /**
     * Replicates the default behavior of the {@link OutputCommitter} used by {@link NullOutputFormat}. The method is
     * part of {@link OutputCommitter} class in Hadoop 2.x, but not 1.x
     * @return true
     */
    @SuppressWarnings("unused")
    public boolean isRecoverySupported() {
      return true;
    }

    /**
     * Replicates the default behavior of the {@link OutputCommitter} used by {@link NullOutputFormat}. The method is
     * part of {@link OutputCommitter} class in Hadoop 2.x, but not 1.x
     */
    @SuppressWarnings("unused")
    public void recoverTask(TaskAttemptContext taskContext) throws IOException { }
  }
}
