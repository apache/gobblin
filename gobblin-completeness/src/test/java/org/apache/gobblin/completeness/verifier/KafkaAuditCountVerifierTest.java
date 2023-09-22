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

package org.apache.gobblin.completeness.verifier;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.completeness.audit.TestAuditClient;
import org.apache.gobblin.configuration.State;

@Test
public class KafkaAuditCountVerifierTest {

  public static final String SOURCE_TIER = "gobblin";
  public static final String REFERENCE_TIERS = "producer";

  public static final String TOTAL_COUNT_REF_TIER_0 = "producer_0";
  public static final String TOTAL_COUNT_REF_TIER_1 = "producer_1";
  public static final String TOTAL_COUNT_REFERENCE_TIERS = TOTAL_COUNT_REF_TIER_0 + "," + TOTAL_COUNT_REF_TIER_1;


  public void testFetch() throws IOException {
    final String topic = "testTopic";
    State props = new State();
    props.setProp(KafkaAuditCountVerifier.SOURCE_TIER, SOURCE_TIER);
    props.setProp(KafkaAuditCountVerifier.REFERENCE_TIERS, REFERENCE_TIERS);
    props.setProp(KafkaAuditCountVerifier.THRESHOLD, ".99");
    TestAuditClient client = new TestAuditClient(props);
    KafkaAuditCountVerifier verifier = new KafkaAuditCountVerifier(props, client);

    // All complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 1000L,
        REFERENCE_TIERS,   1000L
    ));
    // Default threshold
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness));

    // 99.999 % complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 999L,
        REFERENCE_TIERS,   1000L
    ));
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness));

    // <= 99% complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 990L,
        REFERENCE_TIERS,   1000L
    ));
    Assert.assertFalse(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness));
  }

  public void testTotalCountCompleteness() throws IOException {
    final String topic = "testTopic";
    State props = new State();
    props.setProp(KafkaAuditCountVerifier.SOURCE_TIER, SOURCE_TIER);
    props.setProp(KafkaAuditCountVerifier.REFERENCE_TIERS, REFERENCE_TIERS);
    props.setProp(KafkaAuditCountVerifier.TOTAL_COUNT_REFERENCE_TIERS, TOTAL_COUNT_REFERENCE_TIERS);
    props.setProp(KafkaAuditCountVerifier.THRESHOLD, ".99");
    TestAuditClient client = new TestAuditClient(props);
    KafkaAuditCountVerifier verifier = new KafkaAuditCountVerifier(props, client);

    // All complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 1000L,
        REFERENCE_TIERS, 1000L,
        TOTAL_COUNT_REF_TIER_0, 600L,
        TOTAL_COUNT_REF_TIER_1, 400L
    ));
    // Default threshold
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness));

    // 99.999 % complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 999L,
        REFERENCE_TIERS, 1000L,
        TOTAL_COUNT_REF_TIER_0, 600L,
        TOTAL_COUNT_REF_TIER_1, 400L
    ));
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness));

    // <= 99% complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 990L,
        REFERENCE_TIERS, 1000L,
        TOTAL_COUNT_REF_TIER_0, 600L,
        TOTAL_COUNT_REF_TIER_1, 400L
    ));
    Assert.assertFalse(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness));
  }

  public void testEmptyAuditCount() throws IOException {
    final String topic = "testTopic";
    State props = new State();
    props.setProp(KafkaAuditCountVerifier.SOURCE_TIER, SOURCE_TIER);
    props.setProp(KafkaAuditCountVerifier.REFERENCE_TIERS, REFERENCE_TIERS);
    props.setProp(KafkaAuditCountVerifier.TOTAL_COUNT_REFERENCE_TIERS, TOTAL_COUNT_REFERENCE_TIERS);
    props.setProp(KafkaAuditCountVerifier.THRESHOLD, ".99");
    props.setProp(KafkaAuditCountVerifier.COMPLETE_ON_NO_COUNTS, true);
    TestAuditClient client = new TestAuditClient(props);
    KafkaAuditCountVerifier verifier = new KafkaAuditCountVerifier(props, client);

    // Client gets empty audit count
    client.setTierCounts(ImmutableMap.of());

    // Should be complete, since COMPLETE_ON_NO_COUNTS=true
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness));
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness));

    // Check validation for exceptions if reference tier is 0 count. Checking for division of x / 0 case where x != 0.
    // Update watermark if source reports counts but reference counts is 0 due to Kafka not reporting counts
    client.setTierCounts(
        ImmutableMap.of(
            SOURCE_TIER, 990L,
            REFERENCE_TIERS, 0L,
            TOTAL_COUNT_REF_TIER_0, 0L,
            TOTAL_COUNT_REF_TIER_1, 0L
        ));
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L).get(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness));
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L).get(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness));

    // Check validation for exceptions if both source and reference tier is 0 count. Checking for division of 0 / 0 case.
    // If both source and reference tiers are 0, we assume we are complete and update the watermark
    // This is to check the case when one source cluster is reporting counts but not the other source cluster. Resulting in a non-empty map but having 0 for srcCount
    client.setTierCounts(
        ImmutableMap.of(
            SOURCE_TIER, 0L,
            REFERENCE_TIERS, 0L,
            TOTAL_COUNT_REF_TIER_0, 0L,
            TOTAL_COUNT_REF_TIER_1, 0L
        ));
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L).get(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness));
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L).get(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness));
  }

  public void testOneCountFailed() throws IOException {
    final String topic = "testTopic";
    State props = new State();
    props.setProp(KafkaAuditCountVerifier.SOURCE_TIER, SOURCE_TIER);
    props.setProp(KafkaAuditCountVerifier.REFERENCE_TIERS, REFERENCE_TIERS);
    props.setProp(KafkaAuditCountVerifier.TOTAL_COUNT_REFERENCE_TIERS, TOTAL_COUNT_REFERENCE_TIERS);
    props.setProp(KafkaAuditCountVerifier.THRESHOLD, ".99");
    props.setProp(KafkaAuditCountVerifier.COMPLETE_ON_NO_COUNTS, true);
    TestAuditClient client = new TestAuditClient(props);
    KafkaAuditCountVerifier verifier = new KafkaAuditCountVerifier(props, client);

    // Missing total count tier which will throw exception
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 999L,
        REFERENCE_TIERS, 1000L
    ));

    // Classic completeness is still returned, but total is missing
    Assert.assertTrue(verifier.calculateCompleteness(topic, 0L, 0L)
        .get(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness));
    Assert.assertFalse(verifier.calculateCompleteness(topic, 0L, 0L)
        .containsKey(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness));
  }
}
