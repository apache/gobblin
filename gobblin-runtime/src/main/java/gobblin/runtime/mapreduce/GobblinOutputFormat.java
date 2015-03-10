/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.mapreduce;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobLauncherUtils;


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
   * allows the cancel method run to for 5 ms until it does a hard kill on the process. In order to make sure the
   * staging data still gets cleaned-up, the cleanup will take place in the AM.
   */
  public class GobblinOutputCommitter extends OutputCommitter {

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
      LOG.info("Aborting Job: " + jobContext.getJobID() + " with state: " + state);

      Configuration conf = jobContext.getConfiguration();

      URI fsUri = URI.create(conf.get(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
      FileSystem fs = FileSystem.get(fsUri, conf);

      Path mrJobDir =
          new Path(conf.get(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY), conf.get(ConfigurationKeys.JOB_NAME_KEY));
      Path jobInputDir = new Path(mrJobDir, "input");

      if (!fs.exists(jobInputDir) || !fs.getFileStatus(jobInputDir).isDir()) {
        return;
      }

      // Iterate through all files in the jobInputDir, each file should correspond to a serialized wu or mwu
      try {
        for (FileStatus status : fs.listStatus(jobInputDir, new WorkUnitFilter())) {

          Closer workUnitFileCloser = Closer.create();

          // If the file ends with ".wu" de-serialize it into a WorkUnit
          if (status.getPath().getName().endsWith(".wu")) {
            WorkUnit wu = new WorkUnit();
            try {
              wu.readFields(workUnitFileCloser.register(new DataInputStream(fs.open(status.getPath()))));
            } finally {
              workUnitFileCloser.close();
            }
            JobLauncherUtils.cleanStagingData(wu, LOG);
          }

          // If the file ends with ".mwu" de-serialize it into a MultiWorkUnit
          if (status.getPath().getName().endsWith(".mwu")) {
            MultiWorkUnit mwu = new MultiWorkUnit();
            try {
              mwu.readFields(workUnitFileCloser.register(new DataInputStream(fs.open(status.getPath()))));
            } finally {
              workUnitFileCloser.close();
            }
            for (WorkUnit wu : mwu.getWorkUnits()) {
              JobLauncherUtils.cleanStagingData(wu, LOG);
            }
          }
        }
      } finally {
        if (fs.exists(mrJobDir)) {
          fs.delete(mrJobDir, true);
        }
      }
    }

    @Override
    public void abortTask(TaskAttemptContext arg0) throws IOException {
    }

    @Override
    public void commitTask(TaskAttemptContext arg0) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
      return false;
    }

    @Override
    public void setupJob(JobContext arg0) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext arg0) throws IOException {
    }

    /**
     * Replicates the default behavior of the {@link OutputCommitter} used by {@link NullOutputFormat}.
     * @return true
     */
    public boolean isRecoverySupported() {
      return true;
    }

    /**
     * Replicates the default behavior of the {@link OutputCommitter} used by {@link NullOutputFormat}.
     */
    public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    }

    private class WorkUnitFilter implements PathFilter {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".wu") || path.toString().endsWith(".mwu");
      }
    }
  }
}
