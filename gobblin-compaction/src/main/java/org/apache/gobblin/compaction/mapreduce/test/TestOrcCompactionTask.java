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

package org.apache.gobblin.compaction.mapreduce.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Calendar;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.tools.convert.ConvertTool;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import org.apache.gobblin.compaction.mapreduce.CompactionJobConfigurator;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;

import static org.apache.gobblin.compaction.mapreduce.test.TestCompactionTaskUtils.PATH_SEPARATOR;

/**
 * A convenience class for running ORC compaction locally. Particularly useful for local debugging. The method takes as
 * input a resource folder containing Json files containing input records, and an ORC data1.schema file, and generates
 * input ORC files in hourly folders under ${HOME_DIRECTORY}/data using the ORC {@link ConvertTool}. The ORC input data
 * for compaction job is written under: ${HOME_DIRECTORY}/data/tracking/testTopic/hourly/YYYY/MM/DD/HH.
 * The output of the compaction job is written under: ${HOME_DIRECTORY}/data/tracking/testTopic/daily/YYYY/MM/DD.
 * The year, month, day is derived from the header.time field in the input records.
 *
 * Assumptions:
 * <ul>
 *   <li>The input data has the header.time field, which is assumed to be the epoch time in millis</li>
 *   <li>Associated with each json file, the must be a corresponding schema file containing the ORC schema definition. The schema file
 *   must have the same filename (without extension) as the corresponding json file. See the orcCompactionTest resource folder for
 *   an example. </li>
 * </ul>
 *
 * When running the main() method in your IDE, make sure to remove the hive-exec, log4j-over-slf4j and xerces jars from
 * the Project's External Libraries.
 *
 */
public class TestOrcCompactionTask {
  private static final JsonParser PARSER = new JsonParser();
  private static final String HOURLY_SUBDIR = "tracking/testTopic/hourly";
  private static final String JSON_FILE_EXTENSION = "json";
  private static final String TEST_RESOURCE_FOLDER_NAME = "orcCompactionTest";

  public static void main(String[] args) throws Exception {
    File basePath = new File(System.getProperty("user.home"), TEST_RESOURCE_FOLDER_NAME);
    if (basePath.exists()) {
      FileUtils.deleteDirectory(basePath);
    }
    boolean mkdirs = basePath.mkdirs();
    Preconditions.checkArgument(mkdirs, "Unable to create: " + basePath.getAbsolutePath());
    URL resourceURL = TestOrcCompactionTask.class.getClassLoader().getResource(TEST_RESOURCE_FOLDER_NAME);
    Preconditions.checkArgument(resourceURL != null, "Could not find resource: " + TEST_RESOURCE_FOLDER_NAME);
    File resourceDirectory = new File(resourceURL.getFile());

    for (File file: resourceDirectory.listFiles()) {
      if(isJsonFile(file)) {
        createOrcFile(file, basePath.getAbsolutePath());
      }
    }
    EmbeddedGobblin embeddedGobblin =
        TestCompactionTaskUtils.createEmbeddedGobblinCompactionJob("basic", basePath.getAbsolutePath(), "hourly")
            .setConfiguration(CompactionJobConfigurator.COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY,
                TestCompactionOrcJobConfigurator.Factory.class.getName());
    embeddedGobblin.run();
  }

  private static void createOrcFile(File file, String basePath)
      throws IOException, ParseException {
    JsonElement jsonElement;
    try (Reader reader = new InputStreamReader(new FileInputStream(file), Charset.defaultCharset())) {
      jsonElement = PARSER.parse(reader);
    }

    //Get header.time
    long timestamp = jsonElement.getAsJsonObject().get("header").getAsJsonObject().get("time").getAsLong();
    File hourlyPath = new File(getPath(basePath, timestamp));
    if (!hourlyPath.exists()) {
      boolean result = hourlyPath.mkdirs();
      Preconditions.checkArgument(result, "Unable to create: " + hourlyPath.getAbsolutePath());
    }
    String fileNameWithoutExtensions = Files.getNameWithoutExtension(file.getName());
    File schemaFile = new File(file.getParent(), fileNameWithoutExtensions + ".schema");
    String orcSchema = FileUtils.readFileToString(schemaFile, Charset.defaultCharset());
    String orcFileName = hourlyPath.getAbsolutePath() + PATH_SEPARATOR + fileNameWithoutExtensions + ".orc";
    File orcFile = new File(orcFileName);
    //Delete if file already exists
    if (orcFile.exists()) {
      boolean result = orcFile.delete();
      Preconditions.checkArgument(result, "Unable to delete: " + orcFile.getAbsolutePath());
    }
    //Convert to ORC using the corresponding schema
    String[] convertToolArgs = new String[]{"-s", orcSchema, file.getAbsolutePath(), "-o", orcFileName};
    ConvertTool.main(new Configuration(), convertToolArgs);
  }

  /**
   * A helper method that returns the absolute path of the hourly folder given a timestamp and a basePath.
   * @param basePath e.g. /Users/foo/orcCompactionTaskTest
   * @param timestamp the unix timestamp in milliseconds
   * @return the output path of the hourly folder e.g. /Users/foo/orcCompactionTaskTest/hourly/2020/08/20/12
   */
  private static String getPath(String basePath, Long timestamp) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(timestamp);
    String year = Integer.toString(calendar.get(Calendar.YEAR));
    String month = String.format("%02d", calendar.get(Calendar.MONTH));
    String day = String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH));
    String hour = String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY));
    return Joiner.on(PATH_SEPARATOR).join(basePath, HOURLY_SUBDIR, year, month, day, hour);
  }

  private static boolean isJsonFile(File file) {
    return Files.getFileExtension(file.getName()).equals(JSON_FILE_EXTENSION);
  }
}
