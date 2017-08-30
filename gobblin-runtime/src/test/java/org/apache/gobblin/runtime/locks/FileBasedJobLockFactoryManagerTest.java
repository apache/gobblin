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
package org.apache.gobblin.runtime.locks;

import java.io.File;

import org.apache.hadoop.fs.LocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.runtime.api.JobSpec;

/**
 *  Unit tests for {@link FileBasedJobLockFactoryManager}
 */
public class FileBasedJobLockFactoryManagerTest {

  @Test
  public void testGetFactoryConfig() {
    FileBasedJobLockFactoryManager mgr = new FileBasedJobLockFactoryManager();
    Config sysConfig1 = ConfigFactory.empty();
    Assert.assertTrue(mgr.getFactoryConfig(sysConfig1).isEmpty());

    Config sysConfig2 = sysConfig1.withValue("some.prop", ConfigValueFactory.fromAnyRef("test"));
    Assert.assertTrue(mgr.getFactoryConfig(sysConfig2).isEmpty());

    Config sysConfig3 =
        sysConfig2.withValue(FileBasedJobLockFactoryManager.CONFIG_PREFIX + "." + FileBasedJobLockFactory.LOCK_DIR_CONFIG,
                             ConfigValueFactory.fromAnyRef("/tmp"));
    Config factoryCfg3 = mgr.getFactoryConfig(sysConfig3);
    Assert.assertEquals(factoryCfg3.getString(FileBasedJobLockFactory.LOCK_DIR_CONFIG), "/tmp");
  }

  @Test
  public void testGetJobLockFactory() throws Exception {
    final Logger log = LoggerFactory.getLogger("FileBasedJobLockFactoryManagerTest.testGetJobLockFactory");
    FileBasedJobLockFactoryManager mgr = new FileBasedJobLockFactoryManager();

    // Create an instance with default configs
    Config sysConfig1 = ConfigFactory.empty();
    FileBasedJobLockFactory factory1 = mgr.getJobLockFactory(sysConfig1, Optional.of(log));
    Assert.assertTrue(factory1.getLockFileDir().toString().startsWith(FileBasedJobLockFactory.DEFAULT_LOCK_DIR_PREFIX));
    Assert.assertTrue(factory1.getFs() instanceof LocalFileSystem);
    Assert.assertTrue(factory1.getFs().exists(factory1.getLockFileDir()));

    JobSpec js1 = JobSpec.builder("gobblin-test:job1").build();
    FileBasedJobLock lock11 = factory1.getJobLock(js1);
    Assert.assertTrue(lock11.getLockFile().getName().startsWith(FileBasedJobLockFactory.getJobName(js1)));
    Assert.assertTrue(lock11.tryLock());
    lock11.unlock();

    // Lock dir should be deleted after close()
    factory1.close();
    Assert.assertFalse(factory1.getFs().exists(factory1.getLockFileDir()));

    // Create an instance with pre-existing lock dir
    File lockDir = Files.createTempDir();
    Assert.assertTrue(lockDir.exists());
    Config sysConfig2 = ConfigFactory.empty()
        .withValue(FileBasedJobLockFactoryManager.CONFIG_PREFIX + "." + FileBasedJobLockFactory.LOCK_DIR_CONFIG,
                   ConfigValueFactory.fromAnyRef(lockDir.getAbsolutePath()));

    FileBasedJobLockFactory factory2 = mgr.getJobLockFactory(sysConfig2, Optional.of(log));
    Assert.assertEquals(factory2.getLockFileDir().toString(), lockDir.getAbsolutePath());
    Assert.assertTrue(factory2.getFs() instanceof LocalFileSystem);
    Assert.assertTrue(factory2.getFs().exists(factory2.getLockFileDir()));

    // Lock dir should not be removed on close
    factory2.close();
    Assert.assertTrue(factory2.getFs().exists(factory2.getLockFileDir()));
  }

}
