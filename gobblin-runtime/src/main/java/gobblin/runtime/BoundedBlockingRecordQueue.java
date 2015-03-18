/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;

import gobblin.configuration.ConfigurationKeys;


/**
 * A class implementing a bounded blocking queue with timeout for buffering records between a producer and a consumer.
 *
 * <p>
 *   In addition to the normal queue operations, this class also keeps track of the following statistics:
 *
 *   <ul>
 *     <li>Queue size.</li>
 *     <li>Queue fill ratio (queue size/queue capacity).</li>
 *     <li>Put attempt count.</li>
 *     <li>Mean rate of put attempts (puts/sec).</li>
 *     <li>Get attempt count.</li>
 *     <li>Mean rate of get attempts (gets/sec).</li>
 *   </ul>
 * </p>
 *
 * @author ynli
 */
public class BoundedBlockingRecordQueue<T> {

  private final int capacity;
  private final long timeout;
  private final TimeUnit timeoutTimeUnit;
  private final BlockingDeque<T> blockingDeque;

  private final boolean ifCollectStats;
  private final QueueStats queueStats;

  private BoundedBlockingRecordQueue(Builder<T> builder) {
    Preconditions.checkArgument(builder.capacity > 0, "Invalid queue capacity");
    Preconditions.checkArgument(builder.timeout > 0, "Invalid timeout time");

    this.capacity = builder.capacity;
    this.timeout = builder.timeout;
    this.timeoutTimeUnit = builder.timeoutTimeUnit;
    this.blockingDeque = Queues.newLinkedBlockingDeque(builder.capacity);

    this.ifCollectStats = builder.ifCollectStats;
    this.queueStats = new QueueStats(builder.ifCollectStats);
  }

  /**
   * Put a record to the tail of the queue, waiting (up to the configured timeout time)
   * for an empty space to become available.
   *
   * @param record the record to put to the tail of the queue
   * @return whether the record has been successfully put into the queue
   * @throws InterruptedException if interrupted while waiting
   */
  public boolean put(T record)
      throws InterruptedException {
    boolean offered = this.blockingDeque.offer(record, this.timeout, this.timeoutTimeUnit);
    if (this.ifCollectStats) {
      this.queueStats.putsRateMeter.get().mark();
    }
    return offered;
  }

  /**
   * Get a record from the head of the queue, waiting (up to the configured timeout time)
   * for a record to become available.
   *
   * @return the record at the head of the queue, or <code>null</code> if no record is available
   * @throws InterruptedException if interrupted while waiting
   */
  public T get()
      throws InterruptedException {
    T record = this.blockingDeque.poll(this.timeout, this.timeoutTimeUnit);
    if (this.ifCollectStats) {
      this.queueStats.getsRateMeter.get().mark();
    }
    return record;
  }

  /**
   * Get a current snapshot of this queue's statistics encapsulated in a {@link QueueStats} object.
   *
   * @return a current snapshot of this queue's statistics
   */
  public QueueStats stats() {
    return this.queueStats;
  }

  /**
   * Clear the queue.
   */
  public void clear() {
    this.blockingDeque.clear();
  }

  /**
   * Get a new {@link BoundedBlockingRecordQueue.Builder}.
   *
   * @param <T> record type
   * @return a new {@link BoundedBlockingRecordQueue.Builder}
   */
  public static <T> Builder<T> newBuilder() {
    return new Builder<T>();
  }

  /**
   * A builder class for {@link BoundedBlockingRecordQueue}.
   *
   * @param <T> record type
   */
  public static class Builder<T> {

    private int capacity = ConfigurationKeys.DEFAULT_FORK_BRANCH_RECORD_QUEUE_CAPACITY;
    private long timeout = ConfigurationKeys.DEFAULT_FORK_BRANCH_RECORD_QUEUE_TIMEOUT;
    private TimeUnit timeoutTimeUnit = TimeUnit.MILLISECONDS;
    private boolean ifCollectStats = false;

    /**
     * Configure the capacity of the queue.
     *
     * @param capacity the capacity of the queue
     * @return this {@link Builder} instance
     */
    public Builder<T> hasCapacity(int capacity) {
      this.capacity = capacity;
      return this;
    }

    /**
     * Configure the timeout time of queue operations.
     *
     * @param timeout the time timeout time
     * @return this {@link Builder} instance
     */
    public Builder<T> useTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    /**
     * Configure the timeout time unit of queue operations.
     *
     * @param timeoutTimeUnit the time timeout time unit
     * @return this {@link Builder} instance
     */
    public Builder<T> useTimeoutTimeUnit(TimeUnit timeoutTimeUnit) {
      this.timeoutTimeUnit = timeoutTimeUnit;
      return this;
    }

    /**
     * Configure whether to collect queue statistics.
     *
     * @return this {@link Builder} instance
     */
    public Builder<T> collectStats() {
      this.ifCollectStats = true;
      return this;
    }

    /**
     * Build a new {@link BoundedBlockingRecordQueue}.
     *
     * @return the newly built {@link BoundedBlockingRecordQueue}
     */
    public BoundedBlockingRecordQueue<T> build() {
      return new BoundedBlockingRecordQueue<T>(this);
    }
  }

  /**
   * A class for collecting queue statistics.
   *
   * <p>
   *   All statistics will have zero values if collecting of statistics is not enabled.
   * </p>
   */
  public class QueueStats {

    public static final String QUEUE_SIZE = "queueSize";
    public static final String FILL_RATIO = "fillRatio";
    public static final String PUT_ATTEMPT_RATE = "putAttemptRate";
    public static final String GET_ATTEMPT_RATE = "getAttemptRate";
    public static final String PUT_ATTEMPT_COUNT = "putAttemptCount";
    public static final String GET_ATTEMPT_COUNT = "getAttemptCount";

    private final boolean ifCollectStats;
    private final Optional<Gauge<Integer>> queueSizeGauge;
    private final Optional<Gauge<Double>> fillRatioGauge;
    private final Optional<Meter> putsRateMeter;
    private final Optional<Meter> getsRateMeter;

    public QueueStats(boolean ifCollectStats) {
      this.ifCollectStats = ifCollectStats;
      if (ifCollectStats) {
        Gauge<Integer> integerGauge = new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return blockingDeque.size();
          }
        };
        this.queueSizeGauge = Optional.of(integerGauge);

        Gauge<Double> doubleGauge = new Gauge<Double>() {
          @Override
          public Double getValue() {
            return (double) blockingDeque.size() / capacity;
          }
        };
        this.fillRatioGauge = Optional.of(doubleGauge);

        this.putsRateMeter = Optional.of(new Meter());
        this.getsRateMeter = Optional.of(new Meter());
      } else {
        this.queueSizeGauge = Optional.absent();
        this.fillRatioGauge = Optional.absent();
        this.putsRateMeter = Optional.absent();
        this.getsRateMeter = Optional.absent();
      }
    }

    /**
     * Return the queue size.
     *
     * @return the queue size if collecting of statistics is enabled or zero otherwise
     */
    public int queueSize() {
        return this.queueSizeGauge.isPresent() ? this.queueSizeGauge.get().getValue() : 0;
    }

    /**
     * Return the queue fill ratio.
     *
     * @return the queue fill ratio if collecting of statistics is enabled or zero otherwise
     */
    public double fillRatio() {
      return this.fillRatioGauge.isPresent() ? this.fillRatioGauge.get().getValue() : 0d;
    }

    /**
     * Return the rate of put attempts.
     *
     * @return the rate of put attempts if collecting of statistics is enabled or zero otherwise
     */
    public double putAttemptRate() {
      return this.putsRateMeter.isPresent() ? this.putsRateMeter.get().getMeanRate() : 0d;
    }

    /**
     * Return the total count of put attempts.
     *
     * @return the total count of put attempts if collecting of statistics is enabled or zero otherwise
     */
    public long putAttemptCount() {
      return this.putsRateMeter.isPresent() ? this.putsRateMeter.get().getCount() : 0l;
    }

    /**
     * Return the rate of get attempts.
     *
     * @return the rate of get attempts if collecting of statistics is enabled or zero otherwise
     */
    public double getAttemptRate() {
      return this.getsRateMeter.isPresent() ? this.getsRateMeter.get().getMeanRate() : 0d;
    }

    /**
     * Return the total count of get attempts.
     *
     * @return the total count of get attempts if collecting of statistics is enabled or zero otherwise
     */
    public long getAttemptCount() {
      return this.getsRateMeter.isPresent() ? this.getsRateMeter.get().getCount() : 0l;
    }

    /**
     * Register all statistics as {@link com.codahale.metrics.Metric}s with a
     * {@link com.codahale.metrics.MetricRegistry}.
     *
     * @param metricRegistry the {@link com.codahale.metrics.MetricRegistry} to register with
     * @param prefix metric name prefix
     */
    public void registerAll(MetricRegistry metricRegistry, String prefix) {
      if (this.ifCollectStats) {
        metricRegistry.register(MetricRegistry.name(prefix, QUEUE_SIZE), this.queueSizeGauge.get());
        metricRegistry.register(MetricRegistry.name(prefix, FILL_RATIO), this.fillRatioGauge.get());
        metricRegistry.register(MetricRegistry.name(prefix, PUT_ATTEMPT_RATE), this.putsRateMeter.get());
        metricRegistry.register(MetricRegistry.name(prefix, GET_ATTEMPT_RATE), this.getsRateMeter.get());
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("[");
      sb.append(QUEUE_SIZE).append("=").append(queueSize()).append(", ");
      sb.append(FILL_RATIO).append("=").append(fillRatio()).append(", ");
      sb.append(PUT_ATTEMPT_RATE).append("=").append(putAttemptRate()).append(", ");
      sb.append(PUT_ATTEMPT_COUNT).append("=").append(putAttemptCount()).append(", ");
      sb.append(GET_ATTEMPT_RATE).append("=").append(getAttemptRate()).append(", ");
      sb.append(GET_ATTEMPT_COUNT).append("=").append(getAttemptCount()).append("]");
      return sb.toString();
    }
  }
}
