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
package org.apache.gobblin.partitioner;

import java.util.List;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


@Test(groups = {"gobblin.writer.partitioner"})
public class PartitionKeyTest {

  @Test
  public void testIfKeysAreFlattened() {
    PartitionKey _partitionKey = new PartitionKey("repo.name");

    assertEquals(_partitionKey.getFlattenedKey(), "repo_name");
  }

  @Test
  public void testIfKeyIsNested() {
    PartitionKey _partitionKey = new PartitionKey("repo.name");
    PartitionKey _partitionKey1 = new PartitionKey("repo");

    assertTrue(_partitionKey.isNested());
    assertFalse(_partitionKey1.isNested());
  }

  @Test
  public void testIfSubKeysAreGenerated() {
    PartitionKey _partitionKey = new PartitionKey("repo.url.name");
    PartitionKey _partitionKey1 = new PartitionKey("repo.payload.pusher_type");
    PartitionKey _partitionKey2 = new PartitionKey("repo");

    assertEquals(_partitionKey.getSubKeys(), new String[]{"repo", "url", "name"});
    assertEquals(_partitionKey1.getSubKeys(), new String[]{"repo", "payload", "pusher_type"});
    assertEquals(_partitionKey2.getSubKeys(), new String[]{"repo"});
  }

  @Test
  public void testGeneratePartitionKeysFromProperty() {
    List<PartitionKey> _partitionKeys = PartitionKey.partitionKeys("type,payload.pusher_type");

    assertEquals(_partitionKeys.get(0).getFlattenedKey(), "type");
    assertEquals(_partitionKeys.get(1).getFlattenedKey(), "payload_pusher_type");
  }
}