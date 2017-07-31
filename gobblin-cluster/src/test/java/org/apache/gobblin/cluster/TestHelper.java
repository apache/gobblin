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

package org.apache.gobblin.cluster;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

import org.testng.Assert;

import com.google.common.io.Closer;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.JobLauncherUtils;


/**
 * A helper class for Gobblin Cluster related test constants and utility methods.
 *
 * @author Yinan Li
 */
public class TestHelper {

  public static final String TEST_APPLICATION_NAME = "TestApplication";
  public static final String TEST_APPLICATION_ID = "1";
  public static final String TEST_HELIX_INSTANCE_NAME = HelixUtils.getHelixInstanceName("TestInstance", 0);

  public static final String TEST_TASK_RUNNER_ID = "1";

  public static final String TEST_JOB_NAME = "TestJob";
  public static final String TEST_JOB_ID = JobLauncherUtils.newJobId(TEST_JOB_NAME);
  public static final int TEST_TASK_KEY = 0;
  public static final String TEST_TASK_ID = JobLauncherUtils.newTaskId(TEST_JOB_ID, TEST_TASK_KEY);


  public static final String SOURCE_SCHEMA =
      "{\"namespace\":\"example.avro\", \"type\":\"record\", \"name\":\"User\", "
          + "\"fields\":[{\"name\":\"name\", \"type\":\"string\"}, {\"name\":\"favorite_number\",  "
          + "\"type\":\"int\"}, {\"name\":\"favorite_color\", \"type\":\"string\"}]}\n";

  public static final String SOURCE_JSON_DOCS =
      "{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": \"yellow\"}\n"
          + "{\"name\": \"Ben\", \"favorite_number\": 7, \"favorite_color\": \"red\"}\n"
          + "{\"name\": \"Charlie\", \"favorite_number\": 68, \"favorite_color\": \"blue\"}";

  public static final String REL_WRITER_FILE_PATH = "avro";
  public static final String WRITER_FILE_NAME = "foo.avro";

  public static void createSourceJsonFile(File sourceJsonFile) throws IOException {
    Files.createParentDirs(sourceJsonFile);
    Files.write(SOURCE_JSON_DOCS, sourceJsonFile, ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
  }

  public static void assertGenericRecords(File outputAvroFile, Schema schema) throws IOException {
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(outputAvroFile, new GenericDatumReader<GenericRecord>(schema))) {
      Iterator<GenericRecord> iterator = reader.iterator();

      GenericRecord record = iterator.next();
      Assert.assertEquals(record.get("name").toString(), "Alyssa");

      record = iterator.next();
      Assert.assertEquals(record.get("name").toString(), "Ben");

      record = iterator.next();
      Assert.assertEquals(record.get("name").toString(), "Charlie");

      Assert.assertFalse(iterator.hasNext());
    }
  }

  public static CuratorFramework createZkClient(TestingServer testingZKServer, Closer closer)
      throws InterruptedException {
    CuratorFramework curatorFramework =
        closer.register(CuratorFrameworkFactory.newClient(testingZKServer.getConnectString(),
            new RetryOneTime(2000)));
    curatorFramework.start();
    if (! curatorFramework.blockUntilConnected(60, TimeUnit.SECONDS)) {
      throw new RuntimeException("Time out waiting to connect to ZK!");
    }
    return curatorFramework;
  }
}
