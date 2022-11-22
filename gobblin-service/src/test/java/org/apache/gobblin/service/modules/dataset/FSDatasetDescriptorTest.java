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
package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;


public class FSDatasetDescriptorTest {
  @Test
  public void testContains() throws IOException {
    //Ensure descriptor2's path is matched by the regular expression in descriptor1's path
    Config config1 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"));
    FSDatasetDescriptor descriptor1 = new FSDatasetDescriptor(config1);

    Config config2 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/d"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"))
        .withValue(DatasetDescriptorConfigKeys.FORMAT_KEY, ConfigValueFactory.fromAnyRef("avro"))
        .withValue(DatasetDescriptorConfigKeys.CODEC_KEY, ConfigValueFactory.fromAnyRef("gzip"));

    FSDatasetDescriptor descriptor2 = new FSDatasetDescriptor(config2);
    Assert.assertTrue(descriptor1.contains(descriptor2));

    //Add encryption config
    Config encConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.ENCRYPTION_LEVEL_KEY, ConfigValueFactory.fromAnyRef("file"))
        .withValue(DatasetDescriptorConfigKeys.ENCRYPTION_ALGORITHM_KEY, ConfigValueFactory.fromAnyRef("aes_rotating"))
        .atPath(DatasetDescriptorConfigKeys.ENCYPTION_PREFIX);
    Config config3 = config2.withFallback(encConfig);
    FSDatasetDescriptor descriptor3 = new FSDatasetDescriptor(config3);
    Assert.assertTrue(descriptor2.contains(descriptor3));
    Assert.assertTrue(descriptor1.contains(descriptor3));

    //Add partition config
    Config partitionConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY, ConfigValueFactory.fromAnyRef("datetime"))
        .withValue(DatasetDescriptorConfigKeys.PARTITION_PATTERN_KEY, ConfigValueFactory.fromAnyRef("yyyy/MM/dd"))
        .atPath(DatasetDescriptorConfigKeys.PARTITION_PREFIX);
    Config config4 = config3.withFallback(partitionConfig);
    FSDatasetDescriptor descriptor4 = new FSDatasetDescriptor(config4);
    Assert.assertTrue(descriptor3.contains(descriptor4));
    Assert.assertTrue(descriptor2.contains(descriptor4));
    Assert.assertTrue(descriptor1.contains(descriptor4));

    //Add compaction/retention config
    Config miscConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, ConfigValueFactory.fromAnyRef("true"))
        .withValue(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, ConfigValueFactory.fromAnyRef("true"));
    Config config5 = config4.withFallback(miscConfig);
    FSDatasetDescriptor descriptor5 = new FSDatasetDescriptor(config5);
    Assert.assertFalse(descriptor4.contains(descriptor5));
    Assert.assertFalse(descriptor3.contains(descriptor5));
    Assert.assertFalse(descriptor2.contains(descriptor5));
    Assert.assertFalse(descriptor1.contains(descriptor5));

    // Test subpaths
    Config subPathConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c"))
        .withValue(DatasetDescriptorConfigKeys.SUBPATHS_KEY, ConfigValueFactory.fromAnyRef("{e,f,g}"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"));
    FSDatasetDescriptor descriptor6 = new FSDatasetDescriptor(subPathConfig);
    Assert.assertTrue(descriptor1.contains(descriptor6));
    Assert.assertFalse(descriptor2.contains(descriptor6));

    //Test fs.uri
    Config config7 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/d"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"))
        .withValue(DatasetDescriptorConfigKeys.FORMAT_KEY, ConfigValueFactory.fromAnyRef("avro"))
        .withValue(DatasetDescriptorConfigKeys.CODEC_KEY, ConfigValueFactory.fromAnyRef("gzip"))
        .withValue(DatasetDescriptorConfigKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("hdfs://test-cluster:9000"));
    Config config8 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/d"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"))
        .withValue(DatasetDescriptorConfigKeys.FORMAT_KEY, ConfigValueFactory.fromAnyRef("avro"))
        .withValue(DatasetDescriptorConfigKeys.CODEC_KEY, ConfigValueFactory.fromAnyRef("gzip"))
        .withValue(DatasetDescriptorConfigKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("hdfs://test-cluster_1:9000"));
    FSVolumeDatasetDescriptor descriptor7 = new FSVolumeDatasetDescriptor(config7);
    FSVolumeDatasetDescriptor volumeDescriptor = new FSVolumeDatasetDescriptor(config1);
    FSVolumeDatasetDescriptor descriptor8 = new FSVolumeDatasetDescriptor(config8);
    Assert.assertTrue(volumeDescriptor.contains(descriptor7));
    Assert.assertFalse(descriptor7.contains(volumeDescriptor));
    Assert.assertFalse(descriptor8.contains(descriptor7));
  }

  @Test
  public void testContainsMatchingPaths() throws IOException {
    // Paths that match exactly should be accepted, and that should allow glob patterns as input paths for the self serve edges
    Config config1 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"));
    FSDatasetDescriptor descriptor1 = new FSDatasetDescriptor(config1);

    Config config2 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"));

    FSDatasetDescriptor descriptor2 = new FSDatasetDescriptor(config2);
    Assert.assertTrue(descriptor1.contains(descriptor2));
  }

  @Test
  public void testEquals() throws IOException {
    Config config1 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"));
    FSDatasetDescriptor descriptor1 = new FSDatasetDescriptor(config1);

    Config config2 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"));
    FSDatasetDescriptor descriptor2 = new FSDatasetDescriptor(config2);

    Assert.assertTrue(descriptor1.equals(descriptor2));
    Assert.assertTrue(descriptor2.equals(descriptor1));
    Assert.assertEquals(descriptor1.hashCode(), descriptor2.hashCode());

    Config config3 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"))
        .withValue(DatasetDescriptorConfigKeys.FORMAT_KEY, ConfigValueFactory.fromAnyRef("any"))
        .withValue(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, ConfigValueFactory.fromAnyRef("false"));
    FSDatasetDescriptor descriptor3 = new FSDatasetDescriptor(config3);
    Assert.assertTrue(descriptor1.equals(descriptor3));
    Assert.assertEquals(descriptor1.hashCode(), descriptor3.hashCode());

    //Ensure switching booleans between 2 boolean member variables does not produce the same hashcode.
    Config config4 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"))
        .withValue(DatasetDescriptorConfigKeys.FORMAT_KEY, ConfigValueFactory.fromAnyRef("any"))
        .withValue(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, ConfigValueFactory.fromAnyRef("false"))
        .withValue(DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, ConfigValueFactory.fromAnyRef("true"));
    FSDatasetDescriptor descriptor4 = new FSDatasetDescriptor(config4);

    Config config5 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"))
        .withValue(DatasetDescriptorConfigKeys.FORMAT_KEY, ConfigValueFactory.fromAnyRef("any"))
        .withValue(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, ConfigValueFactory.fromAnyRef("true"))
        .withValue(DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, ConfigValueFactory.fromAnyRef("false"));
    FSDatasetDescriptor descriptor5 = new FSDatasetDescriptor(config5);

    Assert.assertFalse(descriptor4.equals(descriptor5));
    Assert.assertNotEquals(descriptor4.hashCode(), descriptor5.hashCode());
  }

  @Test
  public void testInitFails() {
    //Datetime partition type, invalid datetime pattern
    Config config = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("/a/b/c/*"))
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hdfs"));
    Config partitionConfig = ConfigFactory.empty()
        .withValue(DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY, ConfigValueFactory.fromAnyRef("datetime"))
        .withValue(DatasetDescriptorConfigKeys.PARTITION_PATTERN_KEY, ConfigValueFactory.fromAnyRef("BBBB/MM/dd"))
        .atPath(DatasetDescriptorConfigKeys.PARTITION_PREFIX);
    Config config1 = config.withFallback(partitionConfig);
    Assert.assertThrows(IOException.class, () -> new FSDatasetDescriptor(config1));

    //Regex partition type, invalid regular expression
    partitionConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY, ConfigValueFactory.fromAnyRef("regex"))
        .withValue(DatasetDescriptorConfigKeys.PARTITION_PATTERN_KEY, ConfigValueFactory.fromAnyRef("["))
        .atPath(DatasetDescriptorConfigKeys.PARTITION_PREFIX);
    Config config2 = config.withFallback(partitionConfig);
    Assert.assertThrows(IOException.class, () -> new FSDatasetDescriptor(config2));

    //Partition Config with invalid partition type
    partitionConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY, ConfigValueFactory.fromAnyRef("invalidType"))
        .withValue(DatasetDescriptorConfigKeys.PARTITION_PATTERN_KEY, ConfigValueFactory.fromAnyRef("aaaa"))
        .atPath(DatasetDescriptorConfigKeys.PARTITION_PREFIX);
    Config config3 = config.withFallback(partitionConfig);
    Assert.assertThrows(IOException.class, () -> new FSDatasetDescriptor(config3));

    //Encryption config with invalid encryption level
    Config encryptionConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.ENCRYPTION_LEVEL_KEY, ConfigValueFactory.fromAnyRef("aaaa"))
        .atPath(DatasetDescriptorConfigKeys.ENCYPTION_PREFIX);
    Config config4 = config.withFallback(encryptionConfig);
    Assert.assertThrows(IOException.class, () -> new FSDatasetDescriptor(config4));

    encryptionConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.ENCRYPTION_LEVEL_KEY, ConfigValueFactory.fromAnyRef("field"))
        .atPath(DatasetDescriptorConfigKeys.ENCYPTION_PREFIX);
    Config config5 = config.withFallback(encryptionConfig);
    Assert.assertThrows(IOException.class, () -> new FSDatasetDescriptor(config5));

    encryptionConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.ENCRYPTION_LEVEL_KEY, ConfigValueFactory.fromAnyRef("none"))
        .withValue(DatasetDescriptorConfigKeys.ENCRYPTED_FIELDS, ConfigValueFactory.fromAnyRef("field1")).atPath(DatasetDescriptorConfigKeys.ENCYPTION_PREFIX);
    Config config6 = config.withFallback(encryptionConfig);
    Assert.assertThrows(IOException.class, () -> new FSDatasetDescriptor(config6));



  }
}