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
import java.util.List;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.kafka.writer.Kafka08DataWriter;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.util.ConfigUtils;
import gobblin.writer.WriteCallback;


@NotThreadSafe
public class SimpleKafkaSpecExecutorInstanceProducer extends SimpleKafkaSpecExecutorInstance
    implements SpecExecutorInstanceProducer<Spec>, Closeable  {

  // Producer
  protected Kafka08DataWriter<byte[]> _kafka08Producer;

  public SimpleKafkaSpecExecutorInstanceProducer(Config config, Optional<Logger> log) {
    super(config, log);
  }

  public SimpleKafkaSpecExecutorInstanceProducer(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public SimpleKafkaSpecExecutorInstanceProducer(Config config) {
    this(config, Optional.<Logger>absent());
  }

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    SpecExecutorInstanceDataPacket sidp = new SpecExecutorInstanceDataPacket(Verb.ADD,
        addedSpec.getUri(), addedSpec);
     return getKafka08Producer().write(SerializationUtils.serialize(sidp), WriteCallback.EMPTY);
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    SpecExecutorInstanceDataPacket sidp = new SpecExecutorInstanceDataPacket(Verb.UPDATE,
        updatedSpec.getUri(), updatedSpec);
     return getKafka08Producer().write(SerializationUtils.serialize(sidp), WriteCallback.EMPTY);
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI) {
    SpecExecutorInstanceDataPacket sidp = new SpecExecutorInstanceDataPacket(Verb.DELETE,
        deletedSpecURI, null);
     return getKafka08Producer().write(SerializationUtils.serialize(sidp), WriteCallback.EMPTY);
  }

  @Override
  public Future<? extends List<Spec>> listSpecs() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
     _kafka08Producer.close();
  }

  private Kafka08DataWriter<byte[]> getKafka08Producer() {
    if (null == _kafka08Producer) {
      _kafka08Producer = new Kafka08DataWriter<byte[]>(ConfigUtils.configToProperties(_config));
    }
    return _kafka08Producer;
  }
}
