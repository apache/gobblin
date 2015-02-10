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

package gobblin.publisher;

import gobblin.configuration.ConfigurationKeys;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ForkOperatorUtils;


/**
 * A basic implementation of {@link DataPublisher} that publishes the data from the writer
 * output directory to the final job output directory, preserving the directory structure.
 */
public class BaseDataPublisher extends DataPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDataPublisher.class);

  protected final List<FileSystem> fss = Lists.newArrayList();

  public BaseDataPublisher(State state) {
    super(state);
  }

  @Override
  public void initialize()
      throws IOException {
    Configuration conf = new Configuration();
    // Add all job configuration properties so they are picked up by Hadoop
    for (String key : this.state.getPropertyNames()) {
      conf.set(key, this.state.getProp(key));
    }

    int branches = this.state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    // Get a FileSystem instance for each branch
    for (int i = 0; i < branches; i++) {
      URI uri = URI.create(getState()
          .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, branches, i),
              ConfigurationKeys.LOCAL_FS_URI));
      this.fss.add(FileSystem.get(uri, conf));
    }
  }

  @Override
  public void close()
      throws IOException {
    // Nothing to do
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states)
      throws IOException {
    // We need this to collect unique writer output paths as multiple tasks may
    // belong to the same extract and write to the same output directory
    Set<Path> writerOutputPathMoved = Sets.newHashSet();

    for (WorkUnitState workUnitState : states) {
      int branches = workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
      for (int i = 0; i < branches; i++) {
        String writerFilePathKey =
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, branches, i);
        if (!workUnitState.contains(writerFilePathKey)) {
          // Skip this branch as it does not have data output
          continue;
        }

        Path writerOutput = new Path(workUnitState
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, branches, i)),
            workUnitState.getProp(writerFilePathKey));

        Path publisherOutput = new Path(workUnitState.getProp(
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, branches, i)),
            workUnitState.getProp(writerFilePathKey));

        if (writerOutputPathMoved.contains(writerOutput)) {
          // This writer output path has already been moved for another task of the same extract
          continue;
        }

        // Create the parent directory of the final output directory if it does not exist
        if (!this.fss.get(i).exists(publisherOutput.getParent())) {
          this.fss.get(i).mkdirs(publisherOutput.getParent());
        }

        if (this.fss.get(i).exists(publisherOutput)) {
          // The final output directory already exists, check if configured to replace it.
          boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(ForkOperatorUtils
                  .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, branches, i));
          if (!replaceFinalOutputDir) {
            // The final output directory is not configured to be replaced,
            // add the output files to the existing final output directory.
            // TODO: revisit this part when a use case arises
            boolean preserveFileName = workUnitState.getPropAsBoolean(ForkOperatorUtils
                    .getPropertyNameForBranch(ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, branches, i),
                false);
            for (FileStatus status : this.fss.get(i).listStatus(writerOutput)) {
              // Preserve the file name if configured, use specified name otherwise.
              Path outputPath = preserveFileName ? new Path(publisherOutput, workUnitState.getProp(
                  ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, branches, i)))
                  : new Path(publisherOutput, status.getPath().getName());
              if (this.fss.get(i).rename(status.getPath(), outputPath)) {
                LOG.info(String.format("Moved %s to %s", status.getPath(), outputPath));
              } else {
                throw new IOException("Failed to move from " + status.getPath() + " to " + outputPath);
              }
            }

            writerOutputPathMoved.add(writerOutput);
            continue;
          }

          // Delete the final output directory if configured to be replaced
          this.fss.get(i).delete(publisherOutput, true);
        }

        if (this.fss.get(i).exists(writerOutput)) {
          if (this.fss.get(i).rename(writerOutput, publisherOutput)) {
            LOG.info(String.format("Moved %s to %s", writerOutput, publisherOutput));
            writerOutputPathMoved.add(writerOutput);
          } else {
            throw new IOException("Failed to move from " + writerOutput + " to " + publisherOutput);
          }
        }
      }

      // Upon successfully committing the data to the final output directory, set states
      // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
      // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
      workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states)
      throws IOException {
    // Nothing to do
  }
}
