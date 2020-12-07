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

package org.apache.gobblin.hive.policy;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.spec.SimpleHiveSpec;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES;


/**
 * Unit test for {@link HiveRegistrationPolicyBase}
 *
 * @author Ziyang Liu
 */
@Test(groups = {"gobblin.hive"})
public class HiveRegistrationPolicyBaseTest {
  private Path path;

  @Test
  public void testGetHiveSpecs()
      throws IOException {
    State state = new State();
    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME, "db1");
    state.appendToListProp(ADDITIONAL_HIVE_DATABASE_NAMES, "db2");

    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_TABLE_NAME, "tbl1");
    state.appendToListProp(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_TABLE_NAMES, "tbl2,tbl3");

    this.path = new Path(getClass().getResource("/test-hive-table").toString());

    Collection<HiveSpec> specs = new HiveRegistrationPolicyBase(state).getHiveSpecs(this.path);

    Assert.assertEquals(specs.size(), 6);
    Iterator<HiveSpec> iterator = specs.iterator();
    HiveSpec spec = iterator.next();
    examine(spec, "db1", "tbl1");
    spec = iterator.next();
    examine(spec, "db1", "tbl2");
    spec = iterator.next();
    examine(spec, "db1", "tbl3");
    spec = iterator.next();
    examine(spec, "db2", "tbl1");
    spec = iterator.next();
    examine(spec, "db2", "tbl2");
    spec = iterator.next();
    examine(spec, "db2", "tbl3");
  }

  // Testing fetching additional hive databases from config object. Specifically, we verifies if dataset-level DB
  // config could overwrite the same configuration set in the job level.
  public void testGetDatabasesNames() throws Exception {
    State jobState = new State();
    jobState.setProp(ADDITIONAL_HIVE_DATABASE_NAMES, "db1");

    Properties properties = new Properties();
    properties.setProperty(ADDITIONAL_HIVE_DATABASE_NAMES, "db2");
    Config configObj = ConfigUtils.propertiesToConfig(properties);
    HiveRegistrationPolicyBase policyBase = new HiveRegistrationPolicyBase(jobState);
    // Setting the config object manually.
    policyBase.configForTopic = Optional.fromNullable(configObj);

    // Construct a random Path
    File dir = Files.createTempDir();
    dir.deleteOnExit();
    Path dummyPath = new Path(dir.getAbsolutePath() + " /random");

    int dbCount = 0 ;
    for (String dbName : policyBase.getDatabaseNames(dummyPath)) {
      Assert.assertEquals(dbName, "db2");
      dbCount += 1;
    }

    Assert.assertEquals(dbCount, 1);
  }

  @Test
  public void testGetHiveSpecsWithDBFilter()
      throws IOException {
    State state = new State();
    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME, "db1");
    state.appendToListProp(ADDITIONAL_HIVE_DATABASE_NAMES, "db2");

    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_TABLE_NAME, "tbl1");
    state.appendToListProp(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_TABLE_NAMES, "tbl2,tbl3,$PRIMARY_TABLE_col");

    state.appendToListProp("db2." + HiveRegistrationPolicyBase.HIVE_TABLE_NAME, "$PRIMARY_TABLE_col,tbl4,tbl5");

    this.path = new Path(getClass().getResource("/test-hive-table").toString());

    Collection<HiveSpec> specs = new HiveRegistrationPolicyBase(state).getHiveSpecs(this.path);

    Assert.assertEquals(specs.size(), 7);
    Iterator<HiveSpec> iterator = specs.iterator();
    HiveSpec spec = iterator.next();
    examine(spec, "db1", "tbl1");
    spec = iterator.next();
    examine(spec, "db1", "tbl2");
    spec = iterator.next();
    examine(spec, "db1", "tbl3");
    spec = iterator.next();
    examine(spec, "db1", "tbl1_col");
    spec = iterator.next();
    examine(spec, "db2", "tbl1_col");
    spec = iterator.next();
    examine(spec, "db2", "tbl4");
    spec = iterator.next();
    examine(spec, "db2", "tbl5");
  }

  @Test
  public void testTableRegexp()
      throws IOException {
    State state = new State();
    String regexp = ".*test_bucket/(.*)/staging/.*";
    Optional<Pattern> pattern = Optional.of(Pattern.compile(regexp));
    Path path = new Path("s3://test_bucket/topic/staging/2017-10-21/");

    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_DATABASE_REGEX, regexp);

    HiveRegistrationPolicyBase registrationPolicyBase = new HiveRegistrationPolicyBase(state);

    String resultTable = registrationPolicyBase.getDatabaseOrTableName(path, HiveRegistrationPolicyBase.HIVE_DATABASE_NAME, HiveRegistrationPolicyBase.HIVE_DATABASE_REGEX, pattern );

    Assert.assertEquals(resultTable, "topic");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testTableRegexpWithoutGroupShouldFail()
      throws IOException {
    State state = new State();
    String regexp = ".*test_bucket/.*/staging/.*";
    Optional<Pattern> pattern = Optional.of(Pattern.compile(regexp));
    Path path = new Path("s3://test_bucket/topic/staging/2017-10-21/");

    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_DATABASE_REGEX, regexp);

    HiveRegistrationPolicyBase registrationPolicyBase = new HiveRegistrationPolicyBase(state);

    String resultTable = registrationPolicyBase.getDatabaseOrTableName(path, HiveRegistrationPolicyBase.HIVE_DATABASE_NAME, HiveRegistrationPolicyBase.HIVE_DATABASE_REGEX, pattern );

    Assert.assertEquals(resultTable, "topic");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testTableRegexpWithoutMatchShouldFail()
      throws IOException {
    State state = new State();
    String regexp = "^hdfs://(.*)";
    Optional<Pattern> pattern = Optional.of(Pattern.compile(regexp));
    Path path = new Path("s3://test_bucket/topic/staging/2017-10-21/");

    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_DATABASE_REGEX, regexp);

    HiveRegistrationPolicyBase registrationPolicyBase = new HiveRegistrationPolicyBase(state);

    String resultTable = registrationPolicyBase.getDatabaseOrTableName(path, HiveRegistrationPolicyBase.HIVE_DATABASE_NAME, HiveRegistrationPolicyBase.HIVE_DATABASE_REGEX, pattern );

    Assert.assertEquals(resultTable, "topic");
  }

  static void examine(HiveSpec spec, String dbName, String tableName) {
    Assert.assertEquals(spec.getClass(), SimpleHiveSpec.class);
    Assert.assertEquals(spec.getTable().getDbName(), dbName);
    Assert.assertEquals(spec.getTable().getTableName(), tableName);
  }
}
