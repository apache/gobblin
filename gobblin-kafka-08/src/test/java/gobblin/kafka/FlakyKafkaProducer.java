/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * A Flaky Kafka Producer that wraps a real KafkaProducer.
 * Can be configured to throw errors selectively instead of writing to Kafka
 */
public class FlakyKafkaProducer<K,V> extends KafkaProducer<K,V> {


  private final Future<RecordMetadata> nullFuture = new Future<RecordMetadata>() {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return false;
    }

    @Override
    public RecordMetadata get()
        throws InterruptedException, ExecutionException {
      return null;
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return null;
    }
  };

  private final ErrorManager<V> errorManager;

  public FlakyKafkaProducer(Properties properties) {
    super(properties);
    Config config = ConfigFactory.parseProperties(properties);
    errorManager = new ErrorManager(config);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, final Callback callback) {
    boolean error = errorManager.nextError(record.value());
    if (errorManager.nextError(record.value()))
    {
      final Exception e = new Exception();
      callback.onCompletion(null, e);
      return nullFuture;
    }
    else {
      return super.send(record, callback);
    }

  }

}
