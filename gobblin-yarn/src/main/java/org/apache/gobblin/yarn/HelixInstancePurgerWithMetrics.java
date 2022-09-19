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

package org.apache.gobblin.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.helix.HelixAdmin;


@Slf4j
@AllArgsConstructor
public class HelixInstancePurgerWithMetrics {
  private final EventSubmitter eventSubmitter;
  private final long pollingRateMs;
  private static final String PREFIX = "HelixOfflineInstancePurge.";
  public static final String PURGE_FAILURE_EVENT =  PREFIX + "Failure";
  public static final String PURGE_LAGGING_EVENT = PREFIX + "Lagging";
  public static final String PURGE_COMPLETED_EVENT = PREFIX + "Completed";


  /**
   * Blocking call for purging all offline helix instances. Provides boiler plate code for providing periodic updates
   * and sending a GTE if it's an unexpected amount of time.
   *
   * All previous helix instances should be purged on startup. Gobblin task runners are stateless from helix
   * perspective because all important state is persisted separately in Workunit State Store or Watermark store.
   */
  public void purgeAllOfflineInstances(HelixAdmin admin, String clusterName, long laggingThresholdMs, Map<String, String> gteMetadata) {
    CompletableFuture<Void> purgeTask = CompletableFuture.supplyAsync(() -> {
      long offlineDuration = 0; // 0 means all offline instance should be purged.
      admin.purgeOfflineInstances(clusterName, offlineDuration);
      return null;
    });

    long timeToPurgeMs = waitForPurgeCompletion(purgeTask, laggingThresholdMs, Stopwatch.createUnstarted(), gteMetadata);
    log.info("Finished purging offline helix instances. It took timeToPurgeMs={}", timeToPurgeMs);
  }

  @VisibleForTesting
  long waitForPurgeCompletion(CompletableFuture<Void> purgeTask, long laggingThresholdMs, Stopwatch watch,
      Map<String, String> gteMetadata) {
    watch.start();
    try {
      boolean haveSubmittedLaggingEvent = false; //
      while (!purgeTask.isDone()) {
        long elapsedTimeMs = watch.elapsed(TimeUnit.MILLISECONDS);
        log.info("Waiting for helix to purge offline instances. Cannot proceed with execution because purging is a "
            + "non-thread safe call. To disable purging offline instances during startup, change the flag {} "
            + "elapsedTimeMs={}, laggingThresholdMs={}",
            GobblinYarnConfigurationKeys.HELIX_PURGE_OFFLINE_INSTANCES_ENABLED, elapsedTimeMs, laggingThresholdMs);
        if (!haveSubmittedLaggingEvent && elapsedTimeMs > laggingThresholdMs) {
          submitLaggingEvent(elapsedTimeMs, laggingThresholdMs, gteMetadata);
          haveSubmittedLaggingEvent = true;
        }
        Thread.sleep(this.pollingRateMs);
      }

      long timeToPurgeMs = watch.elapsed(TimeUnit.MILLISECONDS);
      if (!haveSubmittedLaggingEvent && timeToPurgeMs > laggingThresholdMs) {
        submitLaggingEvent(timeToPurgeMs, laggingThresholdMs, gteMetadata);
      }

      purgeTask.get(); // check for exceptions
      submitCompletedEvent(timeToPurgeMs, gteMetadata);
      return timeToPurgeMs;
    } catch (ExecutionException | InterruptedException e) {
      log.warn("The call to purge offline helix instances failed. This is not a fatal error because it is not mandatory to "
          + "clean up old helix instances. But repeated failure to purge offline helix instances will cause an accumulation"
          + "of offline helix instances which may cause large delays in future helix calls.", e);
      long timeToPurgeMs = watch.elapsed(TimeUnit.MILLISECONDS);
      submitFailureEvent(timeToPurgeMs, gteMetadata);
      return timeToPurgeMs;
    }
  }

  private void submitFailureEvent(long elapsedTimeMs, Map<String, String> additionalMetadata) {
    if (eventSubmitter != null) {
      GobblinEventBuilder eventBuilder = new GobblinEventBuilder(PURGE_FAILURE_EVENT);
      eventBuilder.addAdditionalMetadata(additionalMetadata);
      eventBuilder.addMetadata("elapsedTimeMs", String.valueOf(elapsedTimeMs));

      log.warn("Submitting GTE because purging offline instances has failed to complete. event={}", eventBuilder);
      eventSubmitter.submit(eventBuilder);
    } else {
      log.warn("Cannot submit {} GTE because eventSubmitter is null", PURGE_FAILURE_EVENT);
    }
  }

  private void submitCompletedEvent(long timeToPurgeMs, Map<String, String> additionalMetadata) {
    if (eventSubmitter != null) {
      GobblinEventBuilder eventBuilder = new GobblinEventBuilder(PURGE_COMPLETED_EVENT);
      eventBuilder.addAdditionalMetadata(additionalMetadata);
      eventBuilder.addMetadata("timeToPurgeMs", String.valueOf(timeToPurgeMs));

      log.info("Submitting GTE because purging offline instances has completed successfully. event={}", eventBuilder);
      eventSubmitter.submit(eventBuilder);
    } else {
      log.warn("Cannot submit {} GTE because eventSubmitter is null", PURGE_COMPLETED_EVENT);
    }
  }

  private void submitLaggingEvent(long elapsedTimeMs, long laggingThresholdMs,
      Map<String, String> additionalMetadata) {
    if (eventSubmitter != null) {
      GobblinEventBuilder eventBuilder = new GobblinEventBuilder(PURGE_LAGGING_EVENT);
      eventBuilder.addAdditionalMetadata(additionalMetadata);
      eventBuilder.addMetadata("elapsedTimeMs", String.valueOf(elapsedTimeMs));
      eventBuilder.addMetadata("laggingThresholdMs", String.valueOf(laggingThresholdMs));

      log.info("Submitting GTE because purging offline instances is lagging and has exceeded lagging threshold. event={}",
          eventBuilder);
      eventSubmitter.submit(eventBuilder);
    } else {
      log.warn("Cannot submit {} GTE because eventSubmitter is null", PURGE_LAGGING_EVENT);
    }
  }
}
