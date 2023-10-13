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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TopicValidatorsTest {
  @Test
  public void testTopicValidators() {
    List<String> allTopics = Arrays.asList(
        "topic1", "topic2", // allowed
        "topic-with.period-in_middle", ".topic-with-period-at-start", "topicWithPeriodAtEnd.", // bad topics
        "topic3", "topic4"); // in deny list
    List<KafkaTopic> topics = allTopics.stream()
        .map(topicName -> new KafkaTopic(topicName, Collections.emptyList())).collect(Collectors.toList());

    SourceState state = new SourceState();

    // Without any topic validators
    List<KafkaTopic> validTopics = new TopicValidators(state).validate(topics);
    Assert.assertEquals(validTopics.size(), 7);

    // Use 2 topic validators: TopicNameValidator and DenyListValidator
    String validatorsToUse = String.join(TopicValidators.VALIDATOR_CLASS_DELIMITER,
        ImmutableList.of(TopicNameValidator.class.getName(), DenyListValidator.class.getName()));
    state.setProp(TopicValidators.VALIDATOR_CLASSES_KEY, validatorsToUse);
    validTopics = new TopicValidators(state).validate(topics);

    Assert.assertEquals(validTopics.size(), 2);
    Assert.assertTrue(validTopics.stream().anyMatch(topic -> topic.getName().equals("topic1")));
    Assert.assertTrue(validTopics.stream().anyMatch(topic -> topic.getName().equals("topic2")));
  }

  // A TopicValidator class to mimic a deny list
  public static class DenyListValidator extends TopicValidatorBase {
    Set<String> denyList = ImmutableSet.of("topic3", "topic4");

    public DenyListValidator(SourceState sourceState) {
      super(sourceState);
    }

    @Override
    public boolean validate(KafkaTopic topic) {
      return !this.denyList.contains(topic.getName());
    }
  }
}
