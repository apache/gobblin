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
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;

import static org.apache.gobblin.hive.HiveMetaStoreClientFactory.HIVE_METASTORE_TOKEN_SIGNATURE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;


public class HiveConfFactoryTest {
  @Test
  public void testHiveConfFactory() throws Exception {
    HiveConf hiveConf = HiveConfFactory.get(Optional.absent(), SharedResourcesBrokerFactory.getImplicitBroker());
    HiveConf hiveConf1 = HiveConfFactory.get(Optional.absent(), SharedResourcesBrokerFactory.getImplicitBroker());
    Assert.assertEquals(hiveConf, hiveConf1);
    // When there's no hcatURI specified, the default hive-site should be loaded.
    Assert.assertTrue(hiveConf.getVar(METASTOREURIS).equals("file:///test"));
    Assert.assertTrue(hiveConf.get(HIVE_METASTORE_TOKEN_SIGNATURE).equals("file:///test"));

    HiveConf hiveConf2 = HiveConfFactory.get(Optional.of("hcat1"), SharedResourcesBrokerFactory.getImplicitBroker());
    HiveConf hiveConf3 = HiveConfFactory.get(Optional.of("hcat1"), SharedResourcesBrokerFactory.getImplicitBroker());
    Assert.assertEquals(hiveConf2, hiveConf3);

    HiveConf hiveConf4 = HiveConfFactory.get(Optional.of("hcat11"), SharedResourcesBrokerFactory.getImplicitBroker());
    Assert.assertNotEquals(hiveConf3, hiveConf4);
    Assert.assertNotEquals(hiveConf4, hiveConf);

    // THe uri should be correctly set.
    Assert.assertEquals(hiveConf3.getVar(METASTOREURIS), "hcat1");
    Assert.assertEquals(hiveConf3.get(HIVE_METASTORE_TOKEN_SIGNATURE), "hcat1");
    Assert.assertEquals(hiveConf4.getVar(METASTOREURIS), "hcat11");
    Assert.assertEquals(hiveConf4.get(HIVE_METASTORE_TOKEN_SIGNATURE), "hcat11");

  }
}