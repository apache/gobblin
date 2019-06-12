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

package org.apache.gobblin.publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.spec.SimpleHiveSpec;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


import static org.apache.gobblin.configuration.ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY;
import static org.apache.gobblin.configuration.ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY;
import static org.apache.gobblin.hive.HiveRegister.HIVE_METASTORE_URI_KEY;
import static org.apache.gobblin.hive.HiveRegister.HIVE_REGISTER_TYPE;


public class HiveRegistrationPublisherTest {

  /**
   * Mainly test the record count basic logic correctness.
   */
  @Test
  public void testPublishData() throws Exception {
    List<WorkUnitState> states = new ArrayList<>();
    WorkUnitState wus1 = new WorkUnitState();
    wus1.setProp(WORK_UNIT_HIGH_WATER_MARK_KEY, 100);
    wus1.setProp(WORK_UNIT_LOW_WATER_MARK_KEY, 50);
    wus1.setProp(ConfigurationKeys.PUBLISHER_DIRS, "/a/b/c");
    wus1.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY, "org.apache.gobblin.publisher.HiveRegistrationPublisherTest$TestHiveRegistrationPolicy");

    WorkUnitState wus2 = new WorkUnitState();
    wus2.setProp(WORK_UNIT_HIGH_WATER_MARK_KEY, 150);
    wus2.setProp(WORK_UNIT_LOW_WATER_MARK_KEY, 100);
    wus2.setProp(ConfigurationKeys.PUBLISHER_DIRS, "/a/b/c");
    wus2.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY, "org.apache.gobblin.publisher.HiveRegistrationPublisherTest$TestHiveRegistrationPolicy");

    WorkUnitState wus3 = new WorkUnitState();
    wus3.setProp(WORK_UNIT_HIGH_WATER_MARK_KEY, 250);
    wus3.setProp(WORK_UNIT_LOW_WATER_MARK_KEY, 180);
    wus3.setProp(ConfigurationKeys.PUBLISHER_DIRS, "/a/b/d");
    wus3.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY, "org.apache.gobblin.publisher.HiveRegistrationPublisherTest$TestHiveRegistrationPolicy");

    states.add(wus1);
    states.add(wus2);
    states.add(wus3);

    // Creating a mock HiveRegistrationPublisher
    State state = new State();
    state.setProp(HIVE_REGISTER_TYPE, TestHiveRegister.class.getName());
    state.setProp(HIVE_METASTORE_URI_KEY, "random");
    HiveRegistrationPublisher publisher = new HiveRegistrationPublisher(state);
    publisher.publishData(states);

    Assert.assertFalse(publisher.pathToRecordCount.isEmpty());
    Assert.assertEquals((long)publisher.pathToRecordCount.get("/a/b/c"), 100);
    Assert.assertEquals((long)publisher.pathToRecordCount.get("/a/b/d"), 70);
  }

  /**
   * A naive implementation of {@link HiveRegister} for testing purpose only.
   * One could modify this class to make it closer to real register.
   *
   */
  public static class TestHiveRegister extends HiveRegister {
    List<HiveSpec> specs;

    public TestHiveRegister(State state, Optional<String> metastoreURI) throws IOException {
      super(state);
    }

    @Override
    protected void registerPath(HiveSpec spec)
        throws IOException {
      this.specs.add(spec);
    }

    @Override
    public boolean createDbIfNotExists(String dbName)
        throws IOException {
      return false;
    }

    @Override
    public boolean createTableIfNotExists(HiveTable table)
        throws IOException {
      return false;
    }

    @Override
    public boolean addPartitionIfNotExists(HiveTable table, HivePartition partition)
        throws IOException {
      return false;
    }

    @Override
    public boolean existsTable(String dbName, String tableName)
        throws IOException {
      return false;
    }

    @Override
    public boolean existsPartition(String dbName, String tableName, List<HiveRegistrationUnit.Column> partitionKeys,
        List<String> partitionValues)
        throws IOException {
      return false;
    }

    @Override
    public void dropTableIfExists(String dbName, String tableName)
        throws IOException {

    }

    @Override
    public void dropPartitionIfExists(String dbName, String tableName, List<HiveRegistrationUnit.Column> partitionKeys,
        List<String> partitionValues)
        throws IOException {

    }

    @Override
    public Optional<HiveTable> getTable(String dbName, String tableName)
        throws IOException {
      return null;
    }

    @Override
    public Optional<HivePartition> getPartition(String dbName, String tableName,
        List<HiveRegistrationUnit.Column> partitionKeys, List<String> partitionValues)
        throws IOException {
      return null;
    }

    @Override
    public void alterTable(HiveTable table)
        throws IOException {

    }

    @Override
    public void alterPartition(HiveTable table, HivePartition partition)
        throws IOException {

    }
  }

  public static class TestHiveRegistrationPolicy implements HiveRegistrationPolicy {
    // Required for reflection purpose.
    public TestHiveRegistrationPolicy(State props) {
    }

    @Override
    public Collection<HiveSpec> getHiveSpecs(Path path)
        throws IOException {

      return ImmutableList.of(new SimpleHiveSpec.Builder<>(path).withTable(null).withPartition(
          Optional.of(new HivePartition.Builder().withPartitionValues(ImmutableList.of("testPart")).build())).build());
    }
  }
}