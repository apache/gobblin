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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.RetryException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.util.retry.RetryerFactory;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_INTERVAL_MS;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIMES;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TYPE;
import static org.apache.gobblin.util.retry.RetryerFactory.RetryType;


/**
 * {@link CommitStep} to perform Iceberg registration.  It is critically important to use the same source-side {@link TableMetadata} observed while
 * listing the source table and the dest-side {@link TableMetadata} observed just prior to that listing of the source table.  Either table may have
 * changed between first calculating the source-to-dest difference and now performing the commit on the destination (herein).  Accordingly, use of
 * now-current metadata could thwart consistency.  Only metadata preserved from the time of the difference calc guarantees correctness.
 *
 *   - if the source table has since changed, we nonetheless use the metadata originally observed, since now-current metadata wouldn't match the
 *     files just copied to dest
 *   - if the dest table has since changed, we reject the commit altogether to force the diff calc to re-start again (in a subsequent execution)
 */
@Slf4j
public class IcebergRegisterStep implements CommitStep {

  // store as string for serializability... TODO: explore whether truly necessary (or we could just as well store as `TableIdentifier`)
  private final String srcTableIdStr; // used merely for naming within trace logging
  private final String destTableIdStr;
  private final TableMetadata readTimeSrcTableMetadata;
  private final TableMetadata justPriorDestTableMetadata;
  private final Properties properties;
  public static final String RETRYER_CONFIG_PREFIX = IcebergDatasetFinder.ICEBERG_DATASET_PREFIX + ".catalog.registration.retries";

  private static final Config RETRYER_FALLBACK_CONFIG = ConfigFactory.parseMap(ImmutableMap.of(
      RETRY_INTERVAL_MS, TimeUnit.SECONDS.toMillis(3L),
      RETRY_TIMES, 5,
      RETRY_TYPE, RetryType.FIXED_ATTEMPT.name()));

  public IcebergRegisterStep(TableIdentifier srcTableId, TableIdentifier destTableId,
      TableMetadata readTimeSrcTableMetadata, TableMetadata justPriorDestTableMetadata,
      Properties properties) {
    this.srcTableIdStr = srcTableId.toString();
    this.destTableIdStr = destTableId.toString();
    this.readTimeSrcTableMetadata = readTimeSrcTableMetadata;
    this.justPriorDestTableMetadata = justPriorDestTableMetadata;
    this.properties = properties;
  }

  @Override
  public boolean isCompleted() throws IOException {
    return false;
  }

  @Override
  public void execute() throws IOException {
    IcebergTable destIcebergTable = createDestinationCatalog().openTable(TableIdentifier.parse(destTableIdStr));
    try {
      TableMetadata currentDestMetadata = destIcebergTable.accessTableMetadata(); // probe... (first access could throw)
      // CRITICAL: verify current dest-side metadata remains the same as observed just prior to first loading source catalog table metadata
      boolean isJustPriorDestMetadataStillCurrent = currentDestMetadata.uuid().equals(justPriorDestTableMetadata.uuid())
          && currentDestMetadata.metadataFileLocation().equals(justPriorDestTableMetadata.metadataFileLocation());
      String determinationMsg = String.format("(just prior) TableMetadata: %s - %s %s= (current) TableMetadata: %s - %s",
          justPriorDestTableMetadata.uuid(), justPriorDestTableMetadata.metadataFileLocation(),
          isJustPriorDestMetadataStillCurrent ? "=" : "!",
          currentDestMetadata.uuid(), currentDestMetadata.metadataFileLocation());
      log.info("~{}~ [destination] {}", destTableIdStr, determinationMsg);

      // NOTE: we originally expected the dest-side catalog to enforce this match, but the client-side `BaseMetastoreTableOperations.commit`
      // uses `==`, rather than `.equals` (value-cmp), and that invariably leads to:
      //   org.apache.iceberg.exceptions.CommitFailedException: Cannot commit: stale table metadata
      if (!isJustPriorDestMetadataStillCurrent) {
        throw new IOException("error: likely concurrent writing to destination: " + determinationMsg);
      }
      Retryer<Void> registerRetryer = createRegisterRetryer();
      registerRetryer.call(() -> {
        destIcebergTable.registerIcebergTable(readTimeSrcTableMetadata, currentDestMetadata);
        return null;
      });
    } catch (IcebergTable.TableNotFoundException tnfe) {
      String msg = "Destination table (with TableMetadata) does not exist: " + tnfe.getMessage();
      log.error(msg);
      throw new IOException(msg, tnfe);
    } catch (ExecutionException executionException) {
      String msg = String.format("Failed to register iceberg table : (src: {%s}) - (dest: {%s})",
                                  this.srcTableIdStr,
                                  this.destTableIdStr);
      log.error(msg, executionException);
      throw new RuntimeException(msg, executionException.getCause());
    } catch (RetryException retryException) {
      String interruptedNote = Thread.currentThread().isInterrupted() ? "... then interrupted" : "";
      String msg = String.format("Failed to register iceberg table : (src: {%s}) - (dest: {%s}) : (retried %d times) %s ",
                                  this.srcTableIdStr,
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

  @Override
  public String toString() {
    return String.format("Registering Iceberg Table: {%s} (dest); (src: {%s})", this.destTableIdStr, this.srcTableIdStr);
  }

  /** Purely because the static `IcebergDatasetFinder.createIcebergCatalog` proved challenging to mock, even w/ `Mockito::mockStatic` */
  @VisibleForTesting
  protected IcebergCatalog createDestinationCatalog() throws IOException {
    return IcebergDatasetFinder.createIcebergCatalog(this.properties, IcebergDatasetFinder.CatalogLocation.DESTINATION);
  }

  private Retryer<Void> createRegisterRetryer() {
    Config config = ConfigFactory.parseProperties(this.properties);
    Config retryerOverridesConfig = config.hasPath(IcebergRegisterStep.RETRYER_CONFIG_PREFIX)
        ? config.getConfig(IcebergRegisterStep.RETRYER_CONFIG_PREFIX)
        : ConfigFactory.empty();

    return RetryerFactory.newInstance(retryerOverridesConfig.withFallback(RETRYER_FALLBACK_CONFIG), Optional.of(new RetryListener() {
      @Override
      public <V> void onRetry(Attempt<V> attempt) {
        if (attempt.hasException()) {
          String msg = String.format("Exception caught while registering iceberg table : (src: {%s}) - (dest: {%s}) : [attempt: %d; %s after start]",
                                      srcTableIdStr,
                                      destTableIdStr,
                                      attempt.getAttemptNumber(),
                                      Duration.ofMillis(attempt.getDelaySinceFirstAttempt()).toString());
          log.warn(msg, attempt.getExceptionCause());
        }
      }
    }));
  }
}
