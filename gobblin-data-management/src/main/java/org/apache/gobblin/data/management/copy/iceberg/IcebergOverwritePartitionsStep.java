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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

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
  private final List<String> base64EncodedDataFiles;
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
   * @param destTableIdStr the identifier of the destination table as a string
   * @param base64EncodedDataFiles [from List<DataFiles>] the serialized data files to be used for replacing partitions
   * @param properties the properties containing configuration
   */
  public IcebergOverwritePartitionsStep(String destTableIdStr, String partitionColName, String partitionValue, List<String> base64EncodedDataFiles, Properties properties) {
    this.destTableIdStr = destTableIdStr;
    this.partitionColName = partitionColName;
    this.partitionValue = partitionValue;
    this.base64EncodedDataFiles = base64EncodedDataFiles;
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

  private List<DataFile> getDataFiles() {
    List<DataFile> dataFiles = new ArrayList<>(base64EncodedDataFiles.size());
    for (String base64EncodedDataFile : base64EncodedDataFiles) {
      dataFiles.add(SerializationUtil.deserializeFromBase64(base64EncodedDataFile));
    }
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