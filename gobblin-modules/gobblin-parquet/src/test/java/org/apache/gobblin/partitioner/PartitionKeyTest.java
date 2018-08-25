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