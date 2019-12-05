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

import java.io.IOException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;


@Test
public class HiveMetastoreClientPoolTest {

  public void testExtraHiveConf()
      throws IOException {
    String additionalHiveConf = "myhive.metastore.sasl.enabled";
    Properties props = new Properties();
    props.setProperty("hive.additionalConfig.targetUri", "test-target");
    props.setProperty("hive.additionalConfig." + additionalHiveConf, "false");

    HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(props, Optional.of("test"));
    Assert.assertNull(pool.getHiveConf().get(additionalHiveConf));

    pool = HiveMetastoreClientPool.get(props, Optional.of("test-target"));
    Assert.assertFalse(Boolean.valueOf(pool.getHiveConf().get(additionalHiveConf)));
  }
}
