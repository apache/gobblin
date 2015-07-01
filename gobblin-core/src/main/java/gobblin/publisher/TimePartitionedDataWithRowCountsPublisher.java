/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;
import gobblin.writer.WriterOutputFormat;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For time partition jobs, writer output directory is
 * $GOBBLIN_WORK_DIR/task-output/{extractId}/{tableName}/{partitionPath},
 * where partition path is the time bucket, e.g., 2015/04/08/15.
 *
 * While moving the file, also places the row count (number of entries contained within the file)
 * into the name of the file such that output.avro becomes output.{rowCount}.avro.
 *
 * Unlike {@link TimePartitionedDataPublisher}, moves each file individually rather than moving
 * directories at a time. This is done so that each task/branch can move the file it created,
 * allowing it to access the row count information and place it into the file name.
 *
 * Publisher output directory is $GOBBLIN_WORK_DIR/job-output/{tableName}/{partitionPath}
 *
 * @author ekrogen
 */
public class TimePartitionedDataWithRowCountsPublisher extends TimePartitionedDataPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedDataWithRowCountsPublisher.class);

  public TimePartitionedDataWithRowCountsPublisher(State state) {
    super(state);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    Set<Path> publisherDirsProcessed = new HashSet<Path>();

    for (WorkUnitState workUnitState : states) {
      for (int branchId = 0; branchId < this.numBranches; branchId++) {

        // The directory where the workUnitState wrote its output data. It is a combination of
        // WRITER_OUTPUT_DIR and WRITER_FILE_PATH
        Path writerOutputDir = WriterUtils.getWriterOutputDir(workUnitState, this.numBranches, branchId);

        if (!this.fss.get(branchId).exists(writerOutputDir)) {
          LOG.warn("WorkUnit " + workUnitState.getId() + " produced no data");
          workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
          break;
        }

        String fileExtension = WriterOutputFormat.valueOf(workUnitState.getProp(ForkOperatorUtils
                .getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, this.numBranches, branchId),
            WriterOutputFormat.AVRO.name())).getExtension();

        String writerOutputFileName = WriterUtils.getWriterFileName(workUnitState, this.numBranches,
            branchId, workUnitState.getId(), fileExtension);

        Path writerOutputFullGlob =
            getWriterOutputGlobMatcher(workUnitState, branchId, writerOutputDir, writerOutputFileName);

        Path writerOutputFilePath = null;
        for (FileStatus status : this.fss.get(branchId).globStatus(writerOutputFullGlob)) {
          if (writerOutputFilePath != null) {
            LOG.warn("Found more than one output for WorkUnit " + workUnitState.getId() + " branch " + branchId
                + " matching glob: " + writerOutputFullGlob);
          } else {
            writerOutputFilePath = status.getPath();
          }
        }
        if (writerOutputFilePath == null) {
          LOG.error("Unable to find output for WorkUnit " + workUnitState.getId() + " branch " + branchId
              + "; expected to find file matching glob: " + writerOutputFullGlob);
          continue;
        }

        // The directory where the final output directory for this job will be placed. It is a combination of
        // DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH
        Path publisherOutputDir = WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId);

        String writerOutputPathStr = writerOutputFilePath.getParent().toString();

        // The suffix at the end of the writer output path, e.g. 2015/06/29/10
        String pathSuffix = writerOutputPathStr.substring(
            writerOutputPathStr.indexOf(writerOutputDir.toString()) + writerOutputDir.toString().length() + 1);

        int recordsWritten = workUnitState.getPropAsInt(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_ROWS_WRITTEN, this.numBranches, branchId));

        // Remove extension, insert number of records written, put extension back on the end
        String outputFileName = writerOutputFileName.substring(0,
            writerOutputFileName.length() - fileExtension.length()) + recordsWritten + "." + fileExtension;

        Path outputPath = new Path(publisherOutputDir, pathSuffix + Path.SEPARATOR + outputFileName);

        boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, branchId));

        if (!publisherDirsProcessed.contains(publisherOutputDir)
            && replaceFinalOutputDir
            && this.fss.get(branchId).exists(publisherOutputDir)) {
          // If the final output directory is configured to be replaced, and we haven't yet
          // processed (deleted) it, and it exists, then delete it to begin replacing it
          this.fss.get(branchId).delete(publisherOutputDir, true);
        }
        publisherDirsProcessed.add(publisherOutputDir);

        this.fss.get(branchId).mkdirs(outputPath.getParent());
        // Move from writer dir to publisher dir, rename to include row count
        if (this.fss.get(branchId).rename(writerOutputFilePath, outputPath)) {
          LOG.info(String.format("Moved %s to %s", writerOutputFilePath, outputPath));
        } else {
          throw new IOException("Failed to move from " + writerOutputFilePath + " to " + outputPath);
        }
      }

      // Upon successfully committing the data to the final output directory, set states
      // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
      // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
      workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  private Path getWriterOutputGlobMatcher(WorkUnitState workUnitState, int branchId, Path writerOutputDir,
      String writerOutputFileName) {

    String partitionLevel = workUnitState.getProp(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_LEVEL, this.numBranches, branchId),
        ConfigurationKeys.DEFAULT_WRITER_PARTITION_LEVEL);
    String partitionPattern = workUnitState.getProp(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_PATTERN, this.numBranches, branchId),
        ConfigurationKeys.DEFAULT_WRITER_PARTITION_PATTERN);

    Path writerOutputDirWithDateGlob = new Path(writerOutputDir, partitionLevel + Path.SEPARATOR
        + partitionPattern.replaceAll("\\w+", "*"));

    return new Path(writerOutputDirWithDateGlob, writerOutputFileName);
  }
}
