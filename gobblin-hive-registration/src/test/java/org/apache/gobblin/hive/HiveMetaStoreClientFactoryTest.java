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
package org.apache.gobblin.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.hive.HiveMetaStoreClientFactory.HIVE_METASTORE_TOKEN_SIGNATURE;


public class HiveMetaStoreClientFactoryTest {
  @Test
  public void testCreate() throws TException {
    HiveConf hiveConf = new HiveConf();
    HiveMetaStoreClientFactory factory = new HiveMetaStoreClientFactory(hiveConf);

    // Since we havE a specified hive-site in the classpath, so have to null it out here to proceed the test
    // The original value it will get if no local hive-site is placed, will be an empty string. 
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "");
    hiveConf.set(HIVE_METASTORE_TOKEN_SIGNATURE, "");
    IMetaStoreClient msc = factory.create();

    String dbName = "test_db";
    String description = "test database";
    String location = "file:/tmp/" + dbName;
    Database db = new Database(dbName, description, location, null);

    msc.dropDatabase(dbName, true, true);
    msc.createDatabase(db);
    db = msc.getDatabase(dbName);
    Assert.assertEquals(db.getName(), dbName);
    Assert.assertEquals(db.getDescription(), description);
    Assert.assertEquals(db.getLocationUri(), location);
  }
}
