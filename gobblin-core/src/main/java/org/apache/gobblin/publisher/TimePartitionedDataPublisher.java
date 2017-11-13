/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.publisher;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;


/**
 * For time partition jobs, writer output directory is
 * $GOBBLIN_WORK_DIR/task-output/{extractId}/{tableName}/{partitionPath},
 * where partition path is the time bucket, e.g., 2015/04/08/15.
 *
 * Publisher output directory is $GOBBLIN_WORK_DIR/job-output/{tableName}/{partitionPath}
 */
public class TimePartitionedDataPublisher extends BaseDataPublisher {

  public TimePartitionedDataPublisher(State state) throws IOException {
    super(state);
  }

  /**
   * This method needs to be overridden for TimePartitionedDataPublisher, since the output folder structure
   * contains timestamp, we have to move the files recursively.
   *
   * For example, move {writerOutput}/2015/04/08/15/output.avro to {publisherOutput}/2015/04/08/15/output.avro
   */
  @Override
  protected void addWriterOutputToExistingDir(Path writerOutput, Path publisherOutput, WorkUnitState workUnitState,
      int branchId, ParallelRunner parallelRunner) throws IOException {

    for (FileStatus status : FileListUtils.listFilesRecursively(this.writerFileSystemByBranches.get(branchId),
        writerOutput)) {
      String filePathStr = status.getPath().toString();
      String pathSuffix =
          filePathStr.substring(filePathStr.indexOf(writerOutput.toString()) + writerOutput.toString().length() + 1);
      Path outputPath = new Path(publisherOutput, pathSuffix);

      WriterUtils.mkdirsWithRecursivePermissionWithRetry(this.publisherFileSystemByBranches.get(branchId), outputPath.getParent(),
            this.permissions.get(branchId), this.retrierConfig);

      movePath(parallelRunner, workUnitState, status.getPath(), outputPath, branchId);
    }
  }

  @Override
  protected DatasetDescriptor createDestinationDescriptor(WorkUnitState state, int branchId) {
    // Get base descriptor
    DatasetDescriptor descriptor = super.createDestinationDescriptor(state, branchId);

    // Decorate with partition prefix
    String propName = ForkOperatorUtils
        .getPropertyNameForBranch(TimeBasedWriterPartitioner.WRITER_PARTITION_PREFIX, numBranches, branchId);
    String timePrefix = state.getProp(propName, "");
    Path pathWithTimePrefix = new Path(descriptor.getName(), timePrefix);
    DatasetDescriptor destination = new DatasetDescriptor(descriptor.getPlatform(), pathWithTimePrefix.toString());
    // Add back the metadata
    descriptor.getMetadata().forEach(destination::addMetadata);

    return destination;
  }
}
