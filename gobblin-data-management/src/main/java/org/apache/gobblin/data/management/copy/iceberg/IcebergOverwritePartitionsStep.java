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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.DataInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.SerializationUtil;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.retry.RetryerFactory;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_INTERVAL_MS;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIMES;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TYPE;
import static org.apache.gobblin.util.retry.RetryerFactory.RetryType;

/**
 * Commit step for overwriting partitions in an Iceberg table.
 * <p>
 * This class implements the {@link CommitStep} interface and provides functionality to overwrite
 * partitions in the destination Iceberg table using serialized data files.
 * </p>
 */
@Slf4j
public class IcebergOverwritePartitionsStep implements CommitStep {
  private final String destTableIdStr;
  private final Properties properties;
  // Data files are kept as a list of base64 encoded strings for optimised de-serialization.
  private final Path base64EncodedDataFilesPath;
  private final String partitionColName;
  private final String partitionValue;
  public static final String OVERWRITE_PARTITIONS_RETRYER_CONFIG_PREFIX = IcebergDatasetFinder.ICEBERG_DATASET_PREFIX +
      ".catalog.overwrite.partitions.retries";
  private static final Config RETRYER_FALLBACK_CONFIG = ConfigFactory.parseMap(ImmutableMap.of(
      RETRY_INTERVAL_MS, TimeUnit.SECONDS.toMillis(3L),
      RETRY_TIMES, 3,
      RETRY_TYPE, RetryType.FIXED_ATTEMPT.name()));

  /**
   * Constructs an {@code IcebergReplacePartitionsStep} with the specified parameters.
   *
   * @param destTableIdStr             the identifier of the destination table as a string
   * @param base64EncodedDataFilesPath base path where all data files are written
   * @param properties                 the properties containing configuration
   */
  public IcebergOverwritePartitionsStep(String destTableIdStr, String partitionColName, String partitionValue, Path base64EncodedDataFilesPath, Properties properties) {
    this.destTableIdStr = destTableIdStr;
    this.partitionColName = partitionColName;
    this.partitionValue = partitionValue;
    this.base64EncodedDataFilesPath = base64EncodedDataFilesPath;
    this.properties = properties;
  }

  @Override
  public boolean isCompleted() {
    return false;
  }

  /**
   * Executes the partition replacement in the destination Iceberg table.
   * Also, have retry mechanism as done in {@link IcebergRegisterStep#execute()}
   *
   * @throws IOException if an I/O error occurs during execution
   */
  @Override
  public void execute() throws IOException {
    // Unlike IcebergRegisterStep::execute, which validates dest table metadata has not changed between copy entity
    // generation and the post-copy commit, do no such validation here, so dest table writes may continue throughout
    // our copying. any new data written in the meanwhile to THE SAME partitions we are about to overwrite will be
    // clobbered and replaced by the copy entities from our execution.
    IcebergTable destTable = createDestinationCatalog().openTable(TableIdentifier.parse(this.destTableIdStr));
    List<DataFile> dataFiles = getDataFiles();
    try {
      log.info("~{}~ Starting partition overwrite - partition: {}; value: {}; numDataFiles: {}; path[0]: {}",
          this.destTableIdStr,
          this.partitionColName,
          this.partitionValue,
          dataFiles.size(),
          dataFiles.get(0).path()
      );
      Retryer<Void> overwritePartitionsRetryer = createOverwritePartitionsRetryer();
      overwritePartitionsRetryer.call(() -> {
        destTable.overwritePartition(dataFiles, this.partitionColName, this.partitionValue);
        return null;
      });
      log.info("~{}~ Successful partition overwrite - partition: {}; value: {}",
          this.destTableIdStr,
          this.partitionColName,
          this.partitionValue
      );
    } catch (ExecutionException executionException) {
      String msg = String.format("~%s~ Failed to overwrite partitions", this.destTableIdStr);
      log.error(msg, executionException);
      throw new RuntimeException(msg, executionException.getCause());
    } catch (RetryException retryException) {
      String interruptedNote = Thread.currentThread().isInterrupted() ? "... then interrupted" : "";
      String msg = String.format("~%s~ Failure attempting to overwrite partition [num failures: %d] %s",
          this.destTableIdStr,
          retryException.getNumberOfFailedAttempts(),
          interruptedNote);
      Throwable informativeException = retryException.getLastFailedAttempt().hasException()
          ? retryException.getLastFailedAttempt().getExceptionCause()
          : retryException;
      log.error(msg, informativeException);
      throw new RuntimeException(msg, informativeException);
    }
  }

  List<DataFile> getDataFiles() throws IOException {
    JobState jobState = new JobState(this.properties);
    FileSystem fs = HadoopUtils.getWriterFileSystem(jobState, 1, 0);

    List<DataFile> dataFiles = Collections.synchronizedList(new ArrayList<>());
    log.info("Reading base64 encoded DataFiles from HDFS path: {}", base64EncodedDataFilesPath);
    RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(base64EncodedDataFilesPath, false);

    List<Path> filePaths = new ArrayList<>();
    while (fileIterator.hasNext()) {
      filePaths.add(fileIterator.next().getPath());
    }
    log.info("Read {} data files path", filePaths.size());
    filePaths.parallelStream().forEach(filePath -> {
      try (DataInputStream in = fs.open(filePath)) {
        String encodedContent = in.readUTF();
        DataFile dataFile = SerializationUtil.deserializeFromBase64(encodedContent);
        dataFiles.add(dataFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    log.info("Total Data files read: {}", dataFiles.size());
    return dataFiles;
  }

  protected IcebergCatalog createDestinationCatalog() throws IOException {
    return IcebergDatasetFinder.createIcebergCatalog(this.properties, IcebergDatasetFinder.CatalogLocation.DESTINATION);
  }

  private Retryer<Void> createOverwritePartitionsRetryer() {
    Config config = ConfigFactory.parseProperties(this.properties);
    Config retryerOverridesConfig = config.hasPath(IcebergOverwritePartitionsStep.OVERWRITE_PARTITIONS_RETRYER_CONFIG_PREFIX)
        ? config.getConfig(IcebergOverwritePartitionsStep.OVERWRITE_PARTITIONS_RETRYER_CONFIG_PREFIX)
        : ConfigFactory.empty();

    return RetryerFactory.newInstance(retryerOverridesConfig.withFallback(RETRYER_FALLBACK_CONFIG), Optional.of(new RetryListener() {
      @Override
      public <V> void onRetry(Attempt<V> attempt) {
        if (attempt.hasException()) {
          String msg = String.format("~%s~ Exception while overwriting partitions [attempt: %d; elapsed: %s]",
              destTableIdStr,
              attempt.getAttemptNumber(),
              Duration.ofMillis(attempt.getDelaySinceFirstAttempt()).toString());
          log.warn(msg, attempt.getExceptionCause());
        }
      }
    }));
  }
}