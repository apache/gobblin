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

package org.apache.gobblin.service;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecConsumer;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.job_monitor.AvroJobSpecKafkaJobMonitor;
import org.apache.gobblin.runtime.job_monitor.KafkaJobMonitor;
import org.apache.gobblin.runtime.std.DefaultJobCatalogListenerImpl;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.SimpleKafkaSpecExecutor.SPEC_KAFKA_TOPICS_KEY;
import lombok.extern.slf4j.Slf4j;

@Slf4j
/**
 * SpecConsumer that consumes from kafka in a streaming manner
 * Implemented {@link AbstractIdleService} for starting up and shutting down.
 */
public class StreamingKafkaSpecConsumer extends AbstractIdleService implements SpecConsumer<Spec>, Closeable {
  public static final String SPEC_STREAMING_BLOCKING_QUEUE_SIZE = "spec.StreamingBlockingQueueSize";
  private static final int DEFAULT_SPEC_STREAMING_BLOCKING_QUEUE_SIZE = 100;
  private final AvroJobSpecKafkaJobMonitor _jobMonitor;
  private final BlockingQueue<ImmutablePair<SpecExecutor.Verb, Spec>> _jobSpecQueue;
  private final MutableJobCatalog _jobCatalog;
  public StreamingKafkaSpecConsumer(Config config, MutableJobCatalog jobCatalog, Optional<Logger> log) {
    String topic = config.getString(SPEC_KAFKA_TOPICS_KEY);
    Config defaults = ConfigFactory.parseMap(ImmutableMap.of(AvroJobSpecKafkaJobMonitor.TOPIC_KEY, topic,
        KafkaJobMonitor.KAFKA_AUTO_OFFSET_RESET_KEY, KafkaJobMonitor.KAFKA_AUTO_OFFSET_RESET_SMALLEST));

    try {
      _jobMonitor = (AvroJobSpecKafkaJobMonitor)(new AvroJobSpecKafkaJobMonitor.Factory())
          .forConfig(config.withFallback(defaults), jobCatalog);
    } catch (IOException e) {
      throw new RuntimeException("Could not create job monitor", e);
    }

    _jobCatalog = jobCatalog;
    _jobSpecQueue = new LinkedBlockingQueue<>(ConfigUtils.getInt(config, "SPEC_STREAMING_BLOCKING_QUEUE_SIZE",
        DEFAULT_SPEC_STREAMING_BLOCKING_QUEUE_SIZE));
  }

  public StreamingKafkaSpecConsumer(Config config, MutableJobCatalog jobCatalog, Logger log) {
    this(config, jobCatalog, Optional.of(log));
  }

  /** Constructor with no logging */
  public StreamingKafkaSpecConsumer(Config config, MutableJobCatalog jobCatalog) {
    this(config, jobCatalog, Optional.<Logger>absent());
  }

  /**
   * This method returns job specs receive from Kafka. It will block if there are no job specs.
   * @return list of (verb, jobspecs) pairs.
   */
  @Override
  public Future<? extends List<Pair<SpecExecutor.Verb, Spec>>> changedSpecs() {
    List<Pair<SpecExecutor.Verb, Spec>> changesSpecs = new ArrayList<>();

    try {
      Pair<SpecExecutor.Verb, Spec> specPair = _jobSpecQueue.take();

      do {
        changesSpecs.add(specPair);

        // if there are more elements then pass them along in this call
        specPair = _jobSpecQueue.poll();
      } while (specPair != null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    return new CompletedFuture(changesSpecs, null);
  }

  @Override
  protected void startUp() {
    // listener will add job specs to a blocking queue to send to callers of changedSpecs()
    // IMPORTANT: This addListener should be invoked after job catalog has been initialized. This is guaranteed because
    //            StreamingKafkaSpecConsumer is boot after jobCatalog in GobblinClusterManager::startAppLauncherAndServices()
    _jobCatalog.addListener(new JobSpecListener());
    _jobMonitor.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() {
    _jobMonitor.stopAsync().awaitTerminated();
  }

  @Override
  public void close() throws IOException {
    shutDown();
  }

  /**
   * JobCatalog listener that puts messages into a blocking queue for consumption by changedSpecs method of
   * {@link StreamingKafkaSpecConsumer}
   */
  protected class JobSpecListener extends DefaultJobCatalogListenerImpl {
    public JobSpecListener() {
      super(StreamingKafkaSpecConsumer.this.log);
    }

    @Override public void onAddJob(JobSpec addedJob) {
      super.onAddJob(addedJob);

      try {
        _jobSpecQueue.put(new ImmutablePair<SpecExecutor.Verb, Spec>(SpecExecutor.Verb.ADD, addedJob));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
      super.onDeleteJob(deletedJobURI, deletedJobVersion);
      try {
        JobSpec.Builder jobSpecBuilder = JobSpec.builder(deletedJobURI);

        Properties props = new Properties();
        jobSpecBuilder.withVersion(deletedJobVersion).withConfigAsProperties(props);

        _jobSpecQueue.put(new ImmutablePair<SpecExecutor.Verb, Spec>(SpecExecutor.Verb.DELETE, jobSpecBuilder.build()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override public void onUpdateJob(JobSpec updatedJob) {
      super.onUpdateJob(updatedJob);

      try {
        _jobSpecQueue.put(new ImmutablePair<SpecExecutor.Verb, Spec>(SpecExecutor.Verb.UPDATE, updatedJob));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}