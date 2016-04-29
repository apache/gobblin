/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.hive.policy;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.State;
import gobblin.hive.spec.HiveSpec;
import gobblin.hive.spec.SimpleHiveSpec;


/**
 * Unit test for {@link HiveRegistrationPolicyBase}
 *
 * @author ziliu
 */
@Test(groups = { "gobblin.hive" })
public class HiveRegistrationPolicyBaseTest {
  private Path path;

  @Test
  public void testGetHiveSpecs() throws IOException {
    State state = new State();
    state.appendToListProp(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME, "db1");
    state.appendToListProp(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES, "db2");

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

  private void examine(HiveSpec spec, String dbName, String tableName) {
    Assert.assertEquals(spec.getClass(), SimpleHiveSpec.class);
    Assert.assertEquals(spec.getTable().getDbName(), dbName);
    Assert.assertEquals(spec.getTable().getTableName(), tableName);
  }
}
