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

package org.apache.gobblin.util.filesystem;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SharedResourcesBrokerImpl;
import org.apache.gobblin.broker.SimpleScopeType;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.DecoratorUtils;


public class FileSystemFactoryTest {

  @Test
  public void test() throws Exception {
    SharedResourcesBrokerImpl<SimpleScopeType> broker = SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    FileSystemKey key = new FileSystemKey(new URI("file:///"), new Configuration());
    FileSystemFactory<SimpleScopeType> factory = new FileSystemFactory<>();

    FileSystem fs =  broker.getSharedResource(factory, key);

    verifyInstrumentedOnce(fs);

    SharedResourcesBroker<SimpleScopeType> subBroker =
        broker.newSubscopedBuilder(SimpleScopeType.LOCAL.defaultScopeInstance()).build();

    FileSystem subBrokerFs =  FileSystemFactory.get(new URI("file:///"), new Configuration(), subBroker);
    Assert.assertEquals(fs, subBrokerFs);
  }

  @Test
  public void testCreationWithInstrumentedScheme() throws Exception {
    SharedResourcesBrokerImpl<SimpleScopeType> broker = SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    FileSystemKey key = new FileSystemKey(new URI("instrumented-file:///"), new Configuration());
    FileSystemFactory<SimpleScopeType> factory = new FileSystemFactory<>();

    FileSystem fs =  broker.getSharedResource(factory, key);

    verifyInstrumentedOnce(fs);
    Assert.assertTrue(DecoratorUtils.resolveUnderlyingObject(fs) instanceof LocalFileSystem);
  }

  @Test
  public void testCreationWithConfigurationFSImpl() throws Exception {
    SharedResourcesBrokerImpl<SimpleScopeType> broker = SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    Configuration conf = new Configuration();
    conf.set("fs.local.impl", InstrumentedLocalFileSystem.class.getName());

    FileSystemKey key = new FileSystemKey(new URI("file:///"), new Configuration());
    FileSystemFactory<SimpleScopeType> factory = new FileSystemFactory<>();

    FileSystem fs =  broker.getSharedResource(factory, key);

    verifyInstrumentedOnce(fs);
    Assert.assertTrue(DecoratorUtils.resolveUnderlyingObject(fs) instanceof LocalFileSystem);
  }

  private void verifyInstrumentedOnce(FileSystem fs) {
    List<Object> list = DecoratorUtils.getDecoratorLineage(fs);
    boolean foundThrottledFs = false;
    for (Object obj : list) {
      if (obj instanceof ThrottledFileSystem) {
        if (foundThrottledFs) {
          Assert.fail("Object instrumented twice.");
        }
        foundThrottledFs = true;
      }
    }
    Assert.assertTrue(foundThrottledFs, "Object not instrumented.");
  }

}
