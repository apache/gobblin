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

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

/**
 * The TopicValidators contains a list of {@link TopicValidatorBase} that validate topics.
 * To enable it, add below settings in the config:
 *   gobblin.kafka.topicValidators=validator1_class_name,validator2_class_name...
 */
@Slf4j
public class TopicValidators {
  public static final String VALIDATOR_CLASSES_KEY = "gobblin.kafka.topicValidators";

  public static final String VALIDATOR_CLASS_DELIMITER = ",";

  private final List<TopicValidatorBase> validators = new ArrayList<>();

  public TopicValidators(SourceState state) {
    String validatorClasses = state.getProp(VALIDATOR_CLASSES_KEY);
    if (Strings.isNullOrEmpty(validatorClasses)) {
      return;
    }

    String[] validatorClassNames = validatorClasses.split(VALIDATOR_CLASS_DELIMITER);
    Arrays.stream(validatorClassNames).forEach(validator -> {
      try {
        this.validators.add(GobblinConstructorUtils.invokeConstructor(TopicValidatorBase.class, validator, state));
      } catch (Exception e) {
        log.error("Failed to create topic validator: {}, due to {}", validator, e);
      }
    });
  }

  /**
   * Validate topics with all the internal validators.
   * Note: the validations for every topic run in parallel.
   * @param topics the topics to be validated
   * @return the topics that pass all the validators
   */
  public List<KafkaTopic> validate(List<KafkaTopic> topics) {
    // Validate the topics in parallel
    return topics.parallelStream()
        .filter(this::validate)
        .collect(Collectors.toList());
  }

  /**
   * Validates a single topic with all the internal validators
   */
  private boolean validate(KafkaTopic topic) {
    log.debug("Validating topic {} in thread: {}", topic, Thread.currentThread().getName());
    for (TopicValidatorBase validator : this.validators) {
      if (!validator.validate(topic)) {
        log.info("Skip KafkaTopic: {}, by validator: {}", topic, validator.getClass().getName());
        return false;
      }
    }
    return true;
  }
}
