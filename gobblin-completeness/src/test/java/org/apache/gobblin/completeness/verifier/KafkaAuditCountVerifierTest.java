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
    Assert.assertTrue(verifier.isComplete(topic, 0L, 0L));

    // 99.999 % complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 999L,
        REFERENCE_TIERS,   1000L
    ));
    Assert.assertTrue(verifier.isComplete(topic, 0L, 0L));

    // <= 99% complete
    client.setTierCounts(ImmutableMap.of(
        SOURCE_TIER, 990L,
        REFERENCE_TIERS,   1000L
    ));
    Assert.assertFalse(verifier.isComplete(topic, 0L, 0L));
  }


}
