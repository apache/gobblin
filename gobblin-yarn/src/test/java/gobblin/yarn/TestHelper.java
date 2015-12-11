/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;

import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.JobLauncherUtils;


/**
 * A helper class for Gobblin Yarn related test constants and utility methods.
 *
 * @author ynli
 */
public class TestHelper {

  static final String TEST_APPLICATION_NAME = "TestApplication";
  static final String TEST_APPLICATION_ID = "application_1447921358856_210676";
  static final String TEST_HELIX_CLUSTER_NAME = "TestCluster";
  static final String TEST_HELIX_INSTANCE_NAME = "TestInstance";

  static final String TEST_CONTROLLER_CONTAINER_ID = "container_1447921358856_210676_01_000001";
  static final String TEST_PARTICIPANT_CONTAINER_ID = "container_1447921358856_210676_01_000002";

  static final String TEST_JOB_NAME = "TestJob";
  static final String TEST_JOB_ID = JobLauncherUtils.newJobId(TEST_JOB_NAME);
  static final String TEST_TASK_ID = JobLauncherUtils.newTaskId(TEST_JOB_ID, 0);

  static final String SOURCE_SCHEMA =
      "{\"namespace\":\"example.avro\", \"type\":\"record\", \"name\":\"User\", "
          + "\"fields\":[{\"name\":\"name\", \"type\":\"string\"}, {\"name\":\"favorite_number\",  "
          + "\"type\":\"int\"}, {\"name\":\"favorite_color\", \"type\":\"string\"}]}\n";

  static final String SOURCE_JSON_DOCS =
      "{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": \"yellow\"}\n"
          + "{\"name\": \"Ben\", \"favorite_number\": 7, \"favorite_color\": \"red\"}\n"
          + "{\"name\": \"Charlie\", \"favorite_number\": 68, \"favorite_color\": \"blue\"}";

  static final String REL_WRITER_FILE_PATH = "avro";
  static final String WRITER_FILE_NAME = "foo.avro";

  static void createSourceJsonFile(File sourceJsonFile) throws IOException {
    Files.createParentDirs(sourceJsonFile);
    Files.write(SOURCE_JSON_DOCS, sourceJsonFile, ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
  }

  static void assertGenericRecords(File outputAvroFile, Schema schema) throws IOException {
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
}
