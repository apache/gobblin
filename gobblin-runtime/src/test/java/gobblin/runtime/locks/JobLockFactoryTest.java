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

package gobblin.runtime.locks;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import com.google.common.io.Closer;
import org.apache.curator.test.TestingServer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;


@Test(groups = {"gobblin.runtime"})
public class JobLockFactoryTest {
  @AfterClass
  public void tearDown() throws IOException {
    ZookeeperBasedJobLock.shutdownCuratorFramework();
  }

  @Test(expectedExceptions = { NullPointerException.class })
  public void testNullProperties_ThrowsException() throws JobLockException, IOException {
    Closer closer = Closer.create();
    try {
      closer.register(JobLockFactory.getJobLock(null, new JobLockEventListener()));
    } finally {
      closer.close();
    }
  }

  @Test(expectedExceptions = { NullPointerException.class })
  public void testNullListener_ThrowsException() throws JobLockException, IOException {
    Closer closer = Closer.create();
    try {
      closer.register(JobLockFactory.getJobLock(new Properties(), null));
    } finally {
      closer.close();
    }
  }

  @Test
  public void testMissingJobLockType_ResultsIn_FileBasedJobLock() throws JobLockException, IOException {
    Closer closer = Closer.create();
    try {
      Properties properties = new Properties();
      properties.setProperty(ConfigurationKeys.FS_URI_KEY, "file:///");
      properties.setProperty(FileBasedJobLock.JOB_LOCK_DIR, "JobLockFactoryTest");
      properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, "JobLockFactoryTest-" + System.currentTimeMillis());
      properties.setProperty(ConfigurationKeys.JOB_LOCK_TYPE, FileBasedJobLock.class.getName());
      JobLock jobLock = closer.register(JobLockFactory.getJobLock(properties, new JobLockEventListener()));
      MatcherAssert.assertThat(jobLock, Matchers.instanceOf(FileBasedJobLock.class));
    } finally {
      closer.close();
    }
  }

  @Test(expectedExceptions = { JobLockException.class })
  public void testInvalidJobLockType_ThrowsException() throws JobLockException, IOException {
    Closer closer = Closer.create();
    try {
      Properties properties = new Properties();
      properties.setProperty(ConfigurationKeys.JOB_LOCK_TYPE, "ThisIsATest");
      JobLock jobLock = closer.register(JobLockFactory.getJobLock(properties, new JobLockEventListener()));
      MatcherAssert.assertThat(jobLock, Matchers.instanceOf(FileBasedJobLock.class));
    } finally {
      closer.close();
    }
  }

  @Test
  public void testGetFileBasedJobLock() throws JobLockException, IOException {
    Closer closer = Closer.create();
    try {
      Properties properties = new Properties();
      properties.setProperty(ConfigurationKeys.FS_URI_KEY, "file:///");
      properties.setProperty(FileBasedJobLock.JOB_LOCK_DIR, "JobLockFactoryTest");
      properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, "JobLockFactoryTest-" + System.currentTimeMillis());
      properties.setProperty(ConfigurationKeys.JOB_LOCK_TYPE, FileBasedJobLock.class.getName());
      JobLock jobLock = closer.register(JobLockFactory.getJobLock(properties, new JobLockEventListener()));
      MatcherAssert.assertThat(jobLock, Matchers.instanceOf(FileBasedJobLock.class));
    } finally {
      closer.close();
    }
  }

  @Test
  public void testGetZookeeperBasedJobLock() throws Exception {
    Closer closer = Closer.create();
    try {
      TestingServer testingServer = closer.register(new TestingServer(11111));
      Properties properties = new Properties();
      properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, "JobLockFactoryTest-" + System.currentTimeMillis());
      properties.setProperty(ConfigurationKeys.JOB_LOCK_TYPE, ZookeeperBasedJobLock.class.getName());
      properties.setProperty(ZookeeperBasedJobLock.CONNECTION_STRING, testingServer.getConnectString());
      properties.setProperty(ZookeeperBasedJobLock.MAX_RETRY_COUNT, "1");
      properties.setProperty(ZookeeperBasedJobLock.LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS, "1000");
      properties.setProperty(ZookeeperBasedJobLock.RETRY_BACKOFF_SECONDS, "1");
      properties.setProperty(ZookeeperBasedJobLock.SESSION_TIMEOUT_SECONDS, "180");
      properties.setProperty(ZookeeperBasedJobLock.CONNECTION_TIMEOUT_SECONDS, "30");
      JobLock jobLock = closer.register(JobLockFactory.getJobLock(properties, new JobLockEventListener()));
      MatcherAssert.assertThat(jobLock, Matchers.instanceOf(ZookeeperBasedJobLock.class));
    } finally {
      closer.close();
    }
  }
}
