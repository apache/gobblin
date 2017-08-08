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

package org.apache.gobblin.config;

import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import org.apache.gobblin.config.common.impl.TestConfigStoreValueInspector;
import org.apache.gobblin.config.store.hdfs.SimpleHdfsConfigStoreTest;


@Test
public class TestEnvironment {

  @BeforeSuite
  public void setup() {
    System.setProperty(SimpleHdfsConfigStoreTest.TAG_NAME_SYS_PROP_KEY, SimpleHdfsConfigStoreTest.TAG_NAME_SYS_PROP_VALUE);
    System.setProperty(TestConfigStoreValueInspector.VALUE_INSPECTOR_SYS_PROP_KEY, TestConfigStoreValueInspector.VALUE_INSPECTOR_SYS_PROP_VALUE);
  }
}
