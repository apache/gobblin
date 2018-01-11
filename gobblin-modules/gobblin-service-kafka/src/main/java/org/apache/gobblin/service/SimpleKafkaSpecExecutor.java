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

import java.io.Serializable;
import java.net.URI;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.spec_executorInstance.AbstractSpecExecutor;
import org.apache.gobblin.util.CompletedFuture;

/**
 * An {@link SpecExecutor} that use Kafka as the communication mechanism.
 */
public class SimpleKafkaSpecExecutor extends AbstractSpecExecutor {
  public static final String SPEC_KAFKA_TOPICS_KEY = "spec.kafka.topics";


  protected static final String VERB_KEY = "Verb";

  private SpecProducer<Spec> specProducer;

  public SimpleKafkaSpecExecutor(Config config, Optional<Logger> log) {
    super(config, log);
    specProducer = new SimpleKafkaSpecProducer(config, log);
  }

  /**
   * Constructor with no logging, necessary for simple use case.
   * @param config
   */
  public SimpleKafkaSpecExecutor(Config config) {
    this(config, Optional.absent());
  }

  @Override
  public Future<? extends SpecProducer> getProducer() {
    return new CompletedFuture<>(this.specProducer, null);
  }

  @Override
  public Future<String> getDescription() {
    return new CompletedFuture<>("SimpleSpecExecutorInstance with URI: " + specExecutorInstanceUri, null);
  }

  @Override
  protected void startUp() throws Exception {
    optionalCloser = Optional.of(Closer.create());
    specProducer = optionalCloser.get().register((SimpleKafkaSpecProducer) specProducer);
  }

  @Override
  protected void shutDown() throws Exception {
    if (optionalCloser.isPresent()) {
      optionalCloser.get().close();
    } else {
      log.warn("There's no Closer existed in " + this.getClass().getName());
    }
  }

  public static class SpecExecutorInstanceDataPacket implements Serializable {

    protected SpecExecutor.Verb _verb;
    protected URI _uri;
    protected Spec _spec;

    public SpecExecutorInstanceDataPacket(SpecExecutor.Verb verb, URI uri, Spec spec) {
      _verb = verb;
      _uri = uri;
      _spec = spec;
    }

    @Override
    public String toString() {
      return String.format("Verb: %s, URI: %s, Spec: %s", _verb, _uri, _spec);
    }
  }
}