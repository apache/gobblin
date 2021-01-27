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

package org.apache.gobblin.source.extractor.extract;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.ack.Ackable;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.MetricContextUtils;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.runtime.StateStoreBasedWatermarkStorage;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.stream.FlushControlMessage;
import org.apache.gobblin.stream.FlushRecordEnvelope;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.LastWatermarkTracker;
import org.apache.gobblin.writer.WatermarkStorage;
import org.apache.gobblin.writer.WatermarkTracker;


/**
 * An abstract class that implements a {@link EventBasedExtractor}. The {@link FlushingExtractor} injects a
 * {@link FlushControlMessage} at a frequency determined by {@value #FLUSH_INTERVAL_SECONDS_KEY}.
 * The {@link FlushControlMessage} blocks further messages being pushed until the outstanding {@link FlushControlMessage}
 * has been acked by the real writer. On a successful ack, the extractor invokes a publish on the underlying DataPublisher,
 * which moves the data from the task output location to the final publish location. After a successful publish, the FlushingExtractor
 * commits the watermarks to the {@link WatermarkStorage}. Note the watermark committed to the watermark storage is the
 * last successfully acked Watermark. Individual extractor implementations should start seeking from the next watermark
 * past this watermark. For example, in the case of a Kafka extractor, the consumer should seek to the offset that is
 * one more than the last committed watermark.
 *
 * Individual extractor implementations that extend the FlushingExtractor need to implement the following method:
 * <ul>
 *   <li>readRecordEnvelopeImpl() - to return the next {@link RecordEnvelope}</li>.
 * </ul>
 *
 * The FlushingExtractor allows applications to plug-in pre and post {@link CommitStep}s as actions to be performed before and after
 * each commit.
 * @param <D> type of {@link RecordEnvelope}
 */

@Slf4j
public abstract class FlushingExtractor<S, D> extends EventBasedExtractor<S, D> {
  public static final String GOBBLIN_EXTRACTOR_PRECOMMIT_STEPS = "gobblin.extractor.precommit.steps";
  public static final String GOBBLIN_EXTRACTOR_POSTCOMMIT_STEPS = "gobblin.extractor.postcommit.steps";
  public static final String FLUSH_INTERVAL_SECONDS_KEY = "stream.flush.interval.secs";
  public static final Long DEFAULT_FLUSH_INTERVAL_SECONDS = 60L;

  public static final String FLUSH_DATA_PUBLISHER_CLASS = "flush.data.publisher.class";
  public static final String DEFAULT_FLUSH_DATA_PUBLISHER_CLASS = "org.apache.gobblin.publisher.BaseDataPublisher";

  public static final String WATERMARK_COMMIT_TIME_METRIC = "state.store.metrics.watermarkCommitTime";
  public static final String COMMIT_STEP_METRIC_PREFIX = "commit.step.";

  @Getter
  protected Map<String, CheckpointableWatermark> lastCommittedWatermarks;

  private final List<String> preCommitSteps;
  private final List<String> postCommitSteps;

  private final Map<String, CommitStep> commitStepMap = Maps.newHashMap();

  private final AtomicLong watermarkCommitTime = new AtomicLong(0L);
  private final List<AtomicLong> preCommitStepTimes = Lists.newArrayList();
  private final List<AtomicLong> postCommitStepTimes = Lists.newArrayList();

  protected Config config;
  @Setter
  private Optional<WatermarkStorage> watermarkStorage;
  @Getter
  protected WatermarkTracker watermarkTracker;
  protected Long flushIntervalMillis;

  protected Long timeOfLastFlush = System.currentTimeMillis();
  private FlushAckable lastFlushAckable;
  private boolean hasOutstandingFlush = false;
  private Optional<DataPublisher> flushPublisher = Optional.absent();
  protected WorkUnitState workUnitState;

  public FlushingExtractor(WorkUnitState state) {
    super(state);
    this.workUnitState = state;
    this.config = ConfigFactory.parseProperties(state.getProperties());
    this.flushIntervalMillis =
        ConfigUtils.getLong(config, FLUSH_INTERVAL_SECONDS_KEY, DEFAULT_FLUSH_INTERVAL_SECONDS) * 1000;
    this.watermarkTracker = new LastWatermarkTracker(false);
    this.watermarkStorage = Optional.of(new StateStoreBasedWatermarkStorage(state));
    this.preCommitSteps = ConfigUtils.getStringList(config, GOBBLIN_EXTRACTOR_PRECOMMIT_STEPS);
    this.postCommitSteps = ConfigUtils.getStringList(config, GOBBLIN_EXTRACTOR_POSTCOMMIT_STEPS);
    preCommitSteps.stream().map(commitStep -> new AtomicLong(0L)).forEach(this.preCommitStepTimes::add);
    postCommitSteps.stream().map(commitStep -> new AtomicLong(0L)).forEach(this.postCommitStepTimes::add);

    initFlushPublisher();
    MetricContextUtils.registerGauge(this.getMetricContext(), WATERMARK_COMMIT_TIME_METRIC, this.watermarkCommitTime);
    initCommitStepMetrics(this.preCommitSteps, this.postCommitSteps);
  }

  private void initCommitStepMetrics(List<String>... commitStepLists) {
    for (List<String> commitSteps : commitStepLists) {
      for (String commitStepAlias : commitSteps) {
        String metricName = COMMIT_STEP_METRIC_PREFIX + commitStepAlias + ".time";
        MetricContextUtils.registerGauge(this.getMetricContext(), metricName, new AtomicLong(0L));
      }
    }
  }

  private StreamEntity<D> generateFlushMessageIfNecessary() {
    Long currentTime = System.currentTimeMillis();
    if ((currentTime - timeOfLastFlush) > this.flushIntervalMillis) {
      return generateFlushMessage(currentTime);
    }
    return null;
  }

  private StreamEntity<D> generateFlushMessage(Long currentTime) {
    log.debug("Injecting flush control message");
    FlushControlMessage<D> flushMessage = FlushControlMessage.<D>builder().flushReason("Timed flush").build();
    FlushAckable flushAckable = new FlushAckable();
    // add a flush ackable to wait for the flush to complete before returning from this flush call
    flushMessage.addCallBack(flushAckable);

    //Preserve the latest flushAckable.
    this.lastFlushAckable = flushAckable;
    this.hasOutstandingFlush = true;
    timeOfLastFlush = currentTime;
    return flushMessage;
  }

  /**
   * Create an {@link DataPublisher} for publishing after a flush. The {@link DataPublisher} is created through a
   * DataPublisherFactory which makes requests
   * to a {@link org.apache.gobblin.broker.iface.SharedResourcesBroker} to support sharing
   * {@link DataPublisher} instances when appropriate.
   * @return the {@link DataPublisher}
   */
  private void initFlushPublisher() {
    if (this.flushPublisher.isPresent()) {
      return;
    }
    String publisherClassName =
        ConfigUtils.getString(this.config, FLUSH_DATA_PUBLISHER_CLASS, DEFAULT_FLUSH_DATA_PUBLISHER_CLASS);

    try {
      this.flushPublisher = (Optional<DataPublisher>) Optional.of(
          GobblinConstructorUtils.invokeLongestConstructor(Class.forName(publisherClassName), this.workUnitState));
    } catch (ReflectiveOperationException e) {
      log.error("Error in instantiating Data Publisher");
      throw new RuntimeException(e);
    }
  }

  @Override
  public StreamEntity<D> readStreamEntityImpl() throws DataRecordException, IOException {
    //Block until an outstanding flush has been Ack-ed.
    if (this.hasOutstandingFlush) {
      Throwable error = this.lastFlushAckable.waitForAck();
      if (error != null) {
        throw new RuntimeException("Error waiting for flush ack", error);
      }

      //Reset outstandingFlush flag
      this.hasOutstandingFlush = false;

      //Run pre-commit steps
      doCommitSequence(preCommitSteps, true);

      //Publish task output to final publish location.
      publishTaskOutput();

      //Provide a callback to the underlying extractor to handle logic for flush ack.
      onFlushAck();

      //Run post-commit steps
      doCommitSequence(postCommitSteps, false);
    }

    StreamEntity<D> entity = generateFlushMessageIfNecessary();
    if (entity != null) {
      return entity;
    }

    //return the next read record.
    RecordEnvelope<D> recordEnvelope = readRecordEnvelopeImpl();
    if (recordEnvelope instanceof FlushRecordEnvelope) {
      StreamEntity<D> flushMessage = generateFlushMessage(System.currentTimeMillis());
      return flushMessage;
    }
    if (recordEnvelope != null) {
      this.watermarkTracker.unacknowledgedWatermark(recordEnvelope.getWatermark());
    }

    return recordEnvelope;
  }

  /**
   * A method that instantiates a {@link CommitStep} given an alias.
   * @param commitStepAlias alias or fully qualified class name of the {@link CommitStep}.
   * @throws IOException
   */
  public CommitStep initCommitStep(String commitStepAlias, boolean isPrecommit) throws IOException {
    return null;
  }

  private void doCommitSequence(List<String> commitSteps, boolean isPrecommit) throws IOException {
    for (int i = 0; i < commitSteps.size(); i++) {
      long startTimeMillis = System.currentTimeMillis();
      String commitStepAlias = commitSteps.get(i);
      CommitStep commitStep = commitStepMap.get(commitStepAlias);
      if (commitStep == null) {
        commitStep = initCommitStep(commitSteps.get(i), isPrecommit);
        commitStepMap.put(commitStepAlias, commitStep);
      }
      log.info("Calling commit step: {}", commitStepAlias);
      commitStep.execute();
      long commitStepTime = System.currentTimeMillis() - startTimeMillis;
      if (isPrecommit) {
        preCommitStepTimes.get(i).set(commitStepTime);
      } else {
        postCommitStepTimes.get(i).set(commitStepTime);
      }
    }
  }

  /**
   * A callback for the underlying extractor to implement logic for handling the completion of a flush. Underlying
   * Extractor can override this method
   */
  protected void onFlushAck() throws IOException {
    checkPointWatermarks();
  }

  /**
   * A method that returns the latest committed watermarks back to the caller. This method will be typically called
   * by the underlying extractor during the initialization phase to retrieve the latest watermarks.
   * @param checkPointableWatermarkClass a {@link CheckpointableWatermark} class
   * @param partitions a collection of partitions assigned to the extractor
   * @return the latest committed watermarks as a map of (source, watermark) pairs. For example, in the case of a KafkaStreamingExtractor,
   * this map would be a collection of (TopicPartition, KafkaOffset) pairs.
   */
  public Map<String, CheckpointableWatermark> getCommittedWatermarks(Class checkPointableWatermarkClass,
      Iterable<String> partitions) {
    Preconditions.checkArgument(CheckpointableWatermark.class.isAssignableFrom(checkPointableWatermarkClass),
        "Watermark class " + checkPointableWatermarkClass.toString() + " is not a CheckPointableWatermark class");
    try {
      this.lastCommittedWatermarks =
          this.watermarkStorage.get().getCommittedWatermarks(checkPointableWatermarkClass, partitions);
    } catch (Exception e) {
      // failed to get watermarks ... log a warning message
      log.warn("Failed to get watermarks... will start from the beginning", e);
      this.lastCommittedWatermarks = Collections.EMPTY_MAP;
    }
    return this.lastCommittedWatermarks;
  }

  /**
   * Publish task output to final publish location.
   */
  protected void publishTaskOutput() throws IOException {
    if (!this.flushPublisher.isPresent()) {
      throw new IOException("Publish called without a flush publisher");
    }
    this.flushPublisher.get().publish(Collections.singletonList(workUnitState));
  }

  @Override
  public void shutdown() {
    // In case hasOutstandingFlush, we need to manually nack the ackable to make sure the CountDownLatch not hang
    if (this.hasOutstandingFlush) {
      this.lastFlushAckable.nack(new IOException("Extractor already shutdown"));
    }
  }

  /**
   * Persist the watermarks in {@link WatermarkTracker#unacknowledgedWatermarks(Map)} to {@link WatermarkStorage}.
   * The method is called when after a {@link FlushControlMessage} has been acknowledged. To make retrieval of
   * the last committed watermarks efficient, this method caches the watermarks present in the unacknowledged watermark
   * map.
   *
   * @throws IOException
   */
  private void checkPointWatermarks() throws IOException {
    Map<String, CheckpointableWatermark> unacknowledgedWatermarks =
        this.watermarkTracker.getAllUnacknowledgedWatermarks();
    if (this.watermarkStorage.isPresent()) {
      long commitBeginTime = System.currentTimeMillis();
      this.watermarkStorage.get().commitWatermarks(unacknowledgedWatermarks.values());
      this.watermarkCommitTime.set(System.currentTimeMillis() - commitBeginTime);
      //Cache the last committed watermarks
      for (Map.Entry<String, CheckpointableWatermark> entry : unacknowledgedWatermarks.entrySet()) {
        this.lastCommittedWatermarks.put(entry.getKey(), entry.getValue());
      }
    } else {
      log.warn("No watermarkStorage found; Skipping checkpointing");
    }
  }

  /**
   * A method to be implemented by the underlying extractor that returns the next record as an instance of
   * {@link RecordEnvelope}
   * @return the next {@link RecordEnvelope} instance read from the source
   */
  public abstract RecordEnvelope<D> readRecordEnvelopeImpl() throws DataRecordException, IOException;

  /**
   * {@link Ackable} for waiting for the flush control message to be processed
   */
  private static class FlushAckable implements Ackable {
    private Throwable error;
    private final CountDownLatch processed;

    public FlushAckable() {
      this.processed = new CountDownLatch(1);
    }

    @Override
    public void ack() {
      this.processed.countDown();
    }

    @Override
    public void nack(Throwable error) {
      this.error = error;
      this.processed.countDown();
    }

    /**
     * Wait for ack
     * @return any error encountered
     */
    public Throwable waitForAck() {
      try {
        this.processed.await();
        return this.error;
      } catch (InterruptedException e) {
        throw new RuntimeException("interrupted while waiting for ack");
      }
    }
  }
}
