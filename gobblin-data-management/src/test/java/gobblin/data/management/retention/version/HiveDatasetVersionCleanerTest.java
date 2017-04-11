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
package gobblin.data.management.retention.version;

import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.data.management.retention.version.HiveDatasetVersionCleaner;


public class HiveDatasetVersionCleanerTest {

  private static Config config = ConfigFactory.parseMap(ImmutableMap.<String, String> of(
      HiveDatasetVersionCleaner.SHOULD_REPLACE_PARTITION_KEY, "true"));

  private static String replacedDb = "db_orc";
  private static String replacedTable = "table_orc";
  private static String replacementDb = "db_avro";
  private static String replacementTable = "table_avro";

  @Test
  public void testShouldReplacePartitionHappyPath() throws IOException {
    // Happy path 1:
    // - Replacement is enabled
    // - Replacement DB, Table names are specified and are different than Replaced DB and Table names
    Assert.assertTrue(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.of(replacementDb), Optional.of(replacementTable)), "Replaced and replacement db / table are different. "
        + "This should have been true. ");

    // Happy path 2:
    // - Replacement is enabled
    // - Replacement DB, Table names are specified and Replaced DB name is different
    Assert.assertTrue(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.of(replacementDb), Optional.of(replacedTable)), "Replaced and replacement db / table are different. "
        + "This should have been true. ");

    // Happy path 3:
    // - Replacement is enabled
    // - Replacement DB, Table names are specified and Replaced Table name is different
    Assert.assertTrue(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.of(replacedDb), Optional.of(replacementTable)), "Replaced and replacement db / table are different. "
        + "This should have been true. ");
  }

  @Test
  public void testShouldReplacePartitionDisabledByConfig() throws IOException {
    Config config = ConfigFactory.parseMap(ImmutableMap.<String, String> of(
        HiveDatasetVersionCleaner.SHOULD_REPLACE_PARTITION_KEY, "false"));

    Assert.assertFalse(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.of(replacementDb), Optional.of(replacementTable)), "Property governing partition replacement is set to false. "
        + "This should have been false. ");
  }

  @Test
  public void testShouldReplacePartitionDisabledByCodePath() throws IOException {
    // Replacement DB and Table names are same as Replaced DB and Table names
    Assert.assertFalse(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.of(replacedDb), Optional.of(replacedTable)), "Replaced and replacement db / table are same. "
        + "This should have been false. ");

    // Replaced DB name is missing
    Assert.assertFalse(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.<String>absent(), Optional.of(replacementTable)), "Replacement DB name is missing. "
        + "This should have been false. ");

    // Replaced Table name is missing
    Assert.assertFalse(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.of(replacementDb), Optional.<String>absent()), "Replacement table name is missing. "
        + "This should have been false. ");

    // Both DB and Table names are missing
    Assert.assertFalse(HiveDatasetVersionCleaner.shouldReplacePartition(config, replacedDb, replacedTable,
        Optional.<String>absent(), Optional.<String>absent()), "Replacement DB and table names are missing. "
        + "This should have been false. ");
  }
}
