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

package gobblin.service;

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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstanceConsumer;
import gobblin.runtime.job_catalog.NonObservingFSJobCatalog;
import gobblin.runtime.job_monitor.AvroJobSpecKafkaJobMonitor;
import gobblin.runtime.job_monitor.KafkaJobMonitor;
import gobblin.runtime.std.DefaultJobCatalogListenerImpl;
import gobblin.util.CompletedFuture;


/**
 * SpecExecutorInstanceConsumer that consumes from kafka in a streaming manner
 */
public class StreamingKafkaSpecExecutorInstanceConsumer extends SimpleKafkaSpecExecutorInstance
    implements SpecExecutorInstanceConsumer<Spec>, Closeable {

  private final AvroJobSpecKafkaJobMonitor _jobMonitor;
  private final NonObservingFSJobCatalog _fsJobCatalog;
  private final BlockingQueue<ImmutablePair<Verb, Spec>> _jobSpecQueue;

  public StreamingKafkaSpecExecutorInstanceConsumer(Config config, Optional<Logger> log) {
    super(config, log);
    String topic = config.getString(SPEC_KAFKA_TOPICS_KEY);
    Config defaults = ConfigFactory.parseMap(ImmutableMap.of(AvroJobSpecKafkaJobMonitor.TOPIC_KEY, topic,
        KafkaJobMonitor.KAFKA_AUTO_OFFSET_RESET_KEY, KafkaJobMonitor.KAFKA_AUTO_OFFSET_RESET_SMALLEST));

    try {
      _fsJobCatalog = new NonObservingFSJobCatalog(config.getConfig("gobblin.cluster"));
      _jobMonitor = (AvroJobSpecKafkaJobMonitor)(new AvroJobSpecKafkaJobMonitor.Factory())
          .forConfig(config.withFallback(defaults), _fsJobCatalog);

      _jobSpecQueue = new LinkedBlockingQueue<>();

      // listener will add job specs to a blocking queue to send to callers of changedSpecs()
      _fsJobCatalog.addListener(new JobSpecListener());

      _fsJobCatalog.startAsync();
      _fsJobCatalog.awaitRunning();
      _jobMonitor.startAsync();
      _jobMonitor.awaitRunning();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public StreamingKafkaSpecExecutorInstanceConsumer(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public StreamingKafkaSpecExecutorInstanceConsumer(Config config) {
    this(config, Optional.<Logger>absent());
  }

  /**
   * This method returns job specs receive from Kafka. It will block if there are no job specs.
   * @return list of (verb, jobspecs) pairs.
   */
  @Override
  public Future<? extends List<Pair<Verb, Spec>>> changedSpecs() {
    List<Pair<Verb, Spec>> changesSpecs = new ArrayList<>();

    try {
      Pair<Verb, Spec> specPair = _jobSpecQueue.take();

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
  public void close() throws IOException {
    _jobMonitor.stopAsync();
    _fsJobCatalog.stopAsync();
    _jobMonitor.awaitTerminated();
    _fsJobCatalog.awaitTerminated();
  }

  /**
   * JobCatalog listener that puts messages into a blocking queue for consumption by changedSpecs method of
   * {@link StreamingKafkaSpecExecutorInstanceConsumer}
   */
  protected class JobSpecListener extends DefaultJobCatalogListenerImpl {
    public JobSpecListener() {
      super(StreamingKafkaSpecExecutorInstanceConsumer.this._log);
    }

    @Override public void onAddJob(JobSpec addedJob) {
      super.onAddJob(addedJob);

      try {
        _jobSpecQueue.put(new ImmutablePair<Verb, Spec>(Verb.ADD, addedJob));
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

        _jobSpecQueue.put(new ImmutablePair<Verb, Spec>(Verb.DELETE, jobSpecBuilder.build()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override public void onUpdateJob(JobSpec updatedJob) {
      super.onUpdateJob(updatedJob);

      try {
        _jobSpecQueue.put(new ImmutablePair<Verb, Spec>(Verb.UPDATE, updatedJob));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}