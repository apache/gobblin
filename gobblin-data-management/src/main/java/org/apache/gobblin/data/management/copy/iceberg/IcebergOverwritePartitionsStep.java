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
  private final byte[] serializedDataFiles;
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
   * @param serializedDataFiles [from List<DataFiles>] the serialized data files to be used for replacing partitions
   * @param properties the properties containing configuration
   */
  public IcebergOverwritePartitionsStep(String destTableIdStr, String partitionColName, String partitionValue, byte[] serializedDataFiles, Properties properties) {
    this.destTableIdStr = destTableIdStr;
    this.partitionColName = partitionColName;
    this.partitionValue = partitionValue;
    this.serializedDataFiles = serializedDataFiles;
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
    IcebergTable destTable = createDestinationCatalog().openTable(TableIdentifier.parse(this.destTableIdStr));
    List<DataFile> dataFiles = SerializationUtil.deserializeFromBytes(this.serializedDataFiles);
    try {
      log.info("Overwriting Data files of partition {} with value {} for destination table : {} ",
          this.partitionColName,
          this.partitionValue,
          this.destTableIdStr
      );
      Retryer<Void> overwritePartitionsRetryer = createOverwritePartitionsRetryer();
      overwritePartitionsRetryer.call(() -> {
        destTable.overwritePartition(dataFiles, this.partitionColName, this.partitionValue);
        return null;
      });
      log.info("Overwriting Data files completed for partition {} with value {} for destination table : {} ",
          this.partitionColName,
          this.partitionValue,
          this.destTableIdStr
      );
    } catch (ExecutionException executionException) {
      String msg = String.format("Failed to overwrite partitions for destination iceberg table : {%s}", this.destTableIdStr);
      log.error(msg, executionException);
      throw new RuntimeException(msg, executionException.getCause());
    } catch (RetryException retryException) {
      String interruptedNote = Thread.currentThread().isInterrupted() ? "... then interrupted" : "";
      String msg = String.format("Failed to overwrite partition for destination table : {%s} : (retried %d times) %s ",
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
          String msg = String.format("Exception caught while overwriting partitions for destination table : {%s} : [attempt: %d; %s after start]",
              destTableIdStr,
              attempt.getAttemptNumber(),
              Duration.ofMillis(attempt.getDelaySinceFirstAttempt()).toString());
          log.warn(msg, attempt.getExceptionCause());
        }
      }
    }));
  }
}