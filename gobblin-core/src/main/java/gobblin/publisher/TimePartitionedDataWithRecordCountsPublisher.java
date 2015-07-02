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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Sets;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;


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
public class TimePartitionedDataWithRecordCountsPublisher extends TimePartitionedDataPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedDataWithRecordCountsPublisher.class);

  public TimePartitionedDataWithRecordCountsPublisher(State state) {
    super(state);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    Set<Path> publisherDirsProcessed = Sets.newHashSet();

    for (WorkUnitState workUnitState : states) {
      for (int branchId = 0; branchId < this.numBranches; branchId++) {

        Path writerOutputDir = WriterUtils.getWriterOutputDir(workUnitState, this.numBranches, branchId);

        FileSystem fileSystem = this.fss.get(branchId);
        if (!fileSystem.exists(writerOutputDir)) {
          LOG.warn("WorkUnit " + workUnitState.getId() + " produced no data");
          workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
          break;
        }

        boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, branchId));

        // The directory where the final output directory for this job will be placed. It is a combination of
        // DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH
        Path publisherOutputDir = WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId);

        if (!publisherDirsProcessed.contains(publisherOutputDir) && replaceFinalOutputDir
            && fileSystem.exists(publisherOutputDir)) {
          // If the final output directory is configured to be replaced, and we haven't yet
          // processed (deleted) it, and it exists, then delete it to begin replacing it
          fileSystem.delete(publisherOutputDir, true);
        }
        publisherDirsProcessed.add(publisherOutputDir);

        List<String> filePathToRecordCountMappings = workUnitState.getPropAsList(ForkOperatorUtils
            .getPropertyNameForBranch("paths.to.record.counts.mapping", this.numBranches, branchId), "");

        if (filePathToRecordCountMappings.isEmpty()) {
          LOG.warn("No record count mappings found for WorkUnit " + workUnitState.getId() + " branch " + branchId);
        }
        for (String filePathToRecordCountMap : filePathToRecordCountMappings) {
          String filePath;
          int recordsWritten;
          try {
            filePath = filePathToRecordCountMap.split(":")[0];
            recordsWritten = Integer.parseInt(filePathToRecordCountMap.split(":")[1]);
          } catch (IndexOutOfBoundsException e) {
            LOG.error("Error parsing filePathToRecordCountMap: " + filePathToRecordCountMap);
            continue;
          } catch (NumberFormatException e) {
            LOG.error("Error parsing filePathToRecordCountMap recordsWritten: " + filePathToRecordCountMap);
            continue;
          }

          moveAndAddRowCounts(writerOutputDir, publisherOutputDir, filePath, recordsWritten, fileSystem);
        }
      }

      // Upon successfully committing the data to the final output directory, set states
      // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
      // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
      workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  /**
   * Moves filePathOld from within the inputDir to outputDir. Also renames the file,
   * adding the number of records written into the file name directly before the extension.
   * For example, if inputDir is /data/staging, outputDir is /data/output,
   * filePathOld is ViewEvent/2015/06/29/taskId42.avro, and recordsWritten is 2048, this will move
   * from /data/staging/ViewEvent/2015/06/29/taskId42.avro
   * to /data/output/ViewEvent/2015/06/29/taskId42.2048.avro
   *
   * @param inputDir Which directory to look within to find filePathOld
   * @param outputDir Which directory to move the newly named file to
   * @param filePathOld Path to the old file within inputDir
   * @param recordsWritten How many records were written to this file
   * @param fileSystem FileSystem to find this file within
   * @throws IOException
   */
  private void moveAndAddRowCounts(Path inputDir, Path outputDir, String filePathOld,
      int recordsWritten, FileSystem fileSystem) throws IOException {

    String filePathNew = filePathOld.substring(0, filePathOld.lastIndexOf(".")) + "."
        + recordsWritten + filePathOld.substring(filePathOld.lastIndexOf("."));

    Path fullPathOld = new Path(inputDir, filePathOld);
    Path fullPathNew = new Path(outputDir, filePathNew);

    fileSystem.mkdirs(fullPathNew.getParent());

    if (fileSystem.rename(fullPathOld, fullPathNew)) {
      LOG.info(String.format("Moved %s to %s", fullPathOld, fullPathNew));
    } else {
      throw new IOException("Failed to move from " + fullPathOld + " to " + fullPathNew);
    }
  }
}
