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
package org.apache.gobblin.runtime.messaging.data;

import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import lombok.extern.java.Log;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

@Log
@Test
public class DynamicWorkUnitSerdeTest {
  @Test
  public void testSerialization() {
    DynamicWorkUnitMessage msg = SplitWorkUnitMessage.builder()
        .workUnitId("workUnitId")
        .laggingTopicPartitions(Arrays.asList("topic-1","topic-2"))
        .build();

    byte[] serializedMsg = DynamicWorkUnitSerde.serialize(msg);

    DynamicWorkUnitMessage deserializedMsg = DynamicWorkUnitSerde.deserialize(serializedMsg);

    assertTrue(deserializedMsg instanceof SplitWorkUnitMessage);
    assertEquals(msg, deserializedMsg);
  }

  @Test(expectedExceptions = DynamicWorkUnitDeserializationException.class)
  public void testSerializationFails() {
    DynamicWorkUnitMessage msg = SplitWorkUnitMessage.builder()
        .workUnitId("workUnitId")
        .laggingTopicPartitions(Arrays.asList("topic-1","topic-2"))
        .build();

    // Serializing without using the DynamicWorkUnitSerde#serialize method should cause a runtime exception
    // when deserializing
    Gson gson = new Gson();
    byte[] serializedMsg = gson.toJson(msg).getBytes(StandardCharsets.UTF_8);

    try {
      DynamicWorkUnitMessage failsToDeserialize = DynamicWorkUnitSerde.deserialize(serializedMsg);
    } catch(DynamicWorkUnitDeserializationException e) {
      log.info("Successfully threw exception when failing to deserialize. exception=" + e);
      throw e;
    }
  }
}
