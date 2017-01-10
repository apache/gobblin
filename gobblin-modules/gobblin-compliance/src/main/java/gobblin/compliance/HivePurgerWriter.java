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
package gobblin.compliance;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import gobblin.writer.DataWriter;


/**
 * This class is responsible for executing all purge queries and altering the partition location if the original
 * partition is not modified during the current execution.
 *
 * @author adsharma
 */
@Slf4j
@AllArgsConstructor
public class HivePurgerWriter implements DataWriter<ComplianceRecord> {
  private FileSystem fs;
  private HivePurgerQueryExecutor hivePurgerQueryExecutor;

  /**
   * This method is responsible for actual purging.
   *  - It first creates a staging table partition with the same schema as of original table partition.
   *  - Staging table partition is then populated by original table left outer joined with compliance id table.
   *
   *  -  Staging partition data will be moved to the final location if the original table is not modified
   *     during the job run.
   *  - Alter query will then change the partition location to the final location.
   *  - In flight queries won't get affected due to alter partition query.
   * @throws IOException
   */
  @Override
  public void write(ComplianceRecord record)
      throws IOException {
    try {
      List<String> purgeQueries = record.getQueries();
      if (record.getCommit()) {
        String finalPartitionLocation = record.getFinalPartitionLocation();
        String stagingPartitionLocation = record.getStagingPartitionLocation();
        String originalPartitionLocation = record.getHivePartition().getLocation();
        String finalDirLocation = record.getFinalDirLocation();
        long lastModificationTimeAtExecutionStart = getLastModifiedTime(originalPartitionLocation);
        long lastModificationTimeAfterQueriesExecution = getLastModifiedTime(originalPartitionLocation);
        hivePurgerQueryExecutor.executeQueries(purgeQueries);

        boolean canCommit =
            commitPolicy(lastModificationTimeAtExecutionStart, lastModificationTimeAfterQueriesExecution);
        if (!canCommit) {
          log.info("Last modified time before start of execution : " + lastModificationTimeAtExecutionStart);
          log.info(
              "Last modified time after execution of purge queries : " + lastModificationTimeAfterQueriesExecution);
          throw new RuntimeException("Failed to commit. File modified during job run.");
        }
        this.fs.mkdirs(new Path(finalDirLocation));
        log.info("Moving from " + stagingPartitionLocation + " to " + finalPartitionLocation);
        this.fs.rename(new Path(stagingPartitionLocation), new Path(finalDirLocation));
        this.hivePurgerQueryExecutor.executeQueries(HivePurgerQueryTemplate.getAlterTableQueries(record));
        return;
      }
      hivePurgerQueryExecutor.executeQueries(purgeQueries);
    } catch (SQLException e) {
      log.info("Failed to execute hive queries : " + e.getMessage());
      throw new IOException(e);
    }
  }

  private long getLastModifiedTime(String file)
      throws IOException {
    return this.fs.getFileStatus(new Path(file)).getModificationTime();
  }

  /**
   * This method checks if the last modification time has changed during the course of execution if the result should be committed.
   */
  private boolean commitPolicy(long oldTime, long newTime) {
    return newTime == oldTime;
  }

  @Override
  public long recordsWritten() {
    return 1;
  }

  /**
   * Following methods are not implemented by this class
   * @throws IOException
   */
  @Override
  public void commit()
      throws IOException {

  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public void cleanup()
      throws IOException {
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return 0;
  }
}
