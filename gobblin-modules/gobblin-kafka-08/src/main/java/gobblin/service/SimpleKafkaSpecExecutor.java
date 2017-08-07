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


import com.google.common.io.Closer;
import gobblin.runtime.api.SpecConsumer;
import gobblin.runtime.api.SpecProducer;
import gobblin.runtime.spec_executorInstance.AbstractSpecExecutor;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.Spec;
import gobblin.util.CompletedFuture;
import gobblin.util.ConfigUtils;
import gobblin.runtime.api.SpecExecutor;


/**
 * An {@link SpecExecutor} that use Kafka as the communication mechanism.
 */
public class SimpleKafkaSpecExecutor extends AbstractSpecExecutor {
  public static final String SPEC_KAFKA_TOPICS_KEY = "spec.kafka.topics";

  // Executor Instance
  protected final URI _specExecutorInstanceUri;

  protected static final String VERB_KEY = "Verb";

  private SpecProducer<Spec> _specProducer;

  private SpecConsumer<Spec> _specConsumer;

  public SimpleKafkaSpecExecutor(Config config, Optional<Logger> log) {
    super(config, log);
    try {
      _specExecutorInstanceUri = new URI(ConfigUtils.getString(config, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,
          "NA"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    _specProducer = new SimpleKafkaSpecProducer(config, log);
    _specConsumer = new SimpleKafkaSpecConsumer(config, log);
  }

  @Override
  public Future<? extends SpecProducer> getProducer() {
    return null;
  }

  @Override
  public URI getUri() {
    return _specExecutorInstanceUri;
  }

  @Override
  public Future<String> getDescription() {
    return new CompletedFuture<>("SimpleSpecExecutorInstance with URI: " + _specExecutorInstanceUri, null);
  }

  @Override
  protected void startUp() throws Exception {
    _optionalCloser = Optional.of(Closer.create());
    _specProducer = _optionalCloser.get().register((SimpleKafkaSpecProducer) _specProducer);
    _specConsumer = _optionalCloser.get().register((SimpleKafkaSpecConsumer) _specConsumer);
  }

  @Override
  protected void shutDown() throws Exception {
    if (_optionalCloser.isPresent()) {
      _optionalCloser.get().close();
    }else{
      throw new RuntimeException("Closer initialization failed");
    }
  }

  public static class SpecExecutorInstanceDataPacket implements Serializable {

    protected Verb _verb;
    protected URI _uri;
    protected Spec _spec;

    public SpecExecutorInstanceDataPacket(Verb verb, URI uri, Spec spec) {
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
