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
package org.apache.gobblin.source.extractor.extract.kafka.validator;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * The TopicValidators contains a list of {@link TopicValidatorBase} that validate topics.
 * To enable it, add below settings in the config:
 *   gobblin.kafka.topicValidators=validator1_class_name,validator2_class_name...
 */
@Slf4j
public class TopicValidators {
  public static final String VALIDATOR_CLASSES_KEY = "gobblin.kafka.topicValidators";

  private static long DEFAULTL_TIMEOUT = 10L;

  private static TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.MINUTES;

  private final List<TopicValidatorBase> validators = new ArrayList<>();

  private final State state;

  public TopicValidators(State state) {
    this.state = state;
    for (String validatorClassName : state.getPropAsList(VALIDATOR_CLASSES_KEY, StringUtils.EMPTY)) {
      try {
        this.validators.add(GobblinConstructorUtils.invokeConstructor(TopicValidatorBase.class, validatorClassName,
            state));
      } catch (Exception e) {
        log.error("Failed to create topic validator: {}, due to {}", validatorClassName, e);
      }
    }
  }

  /**
   * Validate topics with all the internal validators. The default timeout is set to 1 hour.
   * Note:
   *   1. the validations for every topic run in parallel.
   *   2. when timeout happens, un-validated topics are still treated as "valid".
   * @param topics the topics to be validated
   * @return the topics that pass all the validators
   */
  public List<KafkaTopic> validate(List<KafkaTopic> topics) {
    return validate(topics, DEFAULTL_TIMEOUT, DEFAULT_TIMEOUT_UNIT);
  }

  /**
   * Validate topics with all the internal validators.
   * Note:
   *   1. the validations for every topic run in parallel.
   *   2. when timeout happens, un-validated topics are still treated as "valid".
   * @param topics the topics to be validated
   * @param timeout the timeout for the validation
   * @param timeoutUnit the time unit for the timeout
   * @return the topics that pass all the validators
   */
  public List<KafkaTopic> validate(List<KafkaTopic> topics, long timeout, TimeUnit timeoutUnit) {
    int numOfThreads = state.getPropAsInt(ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_THREADS,
        ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_DEFAULT_THREAD_COUNT);

    // Tasks running in the thread pool will have the same access control and class loader settings as current thread
    ExecutorService threadPool = Executors.newFixedThreadPool(numOfThreads, ExecutorsUtils.newPrivilegedThreadFactory(
        Optional.of(log)));

    List<Future<Boolean>> results = new ArrayList<>();
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (KafkaTopic topic : topics) {
      results.add(threadPool.submit(() -> validate(topic)));
    }
    ExecutorsUtils.shutdownExecutorService(threadPool, Optional.of(log), timeout, timeoutUnit);
    log.info(String.format("Validate %d topics in %d seconds", topics.size(), stopwatch.elapsed(TimeUnit.SECONDS)));

    List<KafkaTopic> validTopics = new ArrayList<>();
    for (int i = 0; i < results.size(); ++i) {
      try {
        if (results.get(i).get()) {
          validTopics.add(topics.get(i));
        }
      } catch (InterruptedException | ExecutionException e) {
        log.warn("Failed to validate topic: {}, treat it as a valid topic", topics.get(i));
        validTopics.add(topics.get(i));
      }
    }
    return validTopics;
  }

  /**
   * Validates a single topic with all the internal validators
   */
  private boolean validate(KafkaTopic topic) throws Exception {
    log.info("Validating topic {} in thread: {}", topic, Thread.currentThread().getName());
    for (TopicValidatorBase validator : this.validators) {
      if (!validator.validate(topic)) {
        log.warn("KafkaTopic: {} doesn't pass the validator: {}", topic, validator.getClass().getName());
        return false;
      }
    }
    return true;
  }
}
