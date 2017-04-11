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

package gobblin.compaction.hive;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.compaction.CliOptions;
import gobblin.compaction.mapreduce.MRCompactionRunner;

/**
 * Run Hive compaction based on config files.
 */
public class CompactionRunner {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionRunner.class);

  private static final String COMPACTION_CONFIG_DIR = "compaction.config.dir";
  private static final String TIMING_FILE = "timing.file";
  private static final String TIMING_FILE_DEFAULT = "time.txt";
  private static final String SNAPSHOT = "snapshot";
  private static final String DELTA = "delta";
  private static final String NAME = ".name";
  private static final String PKEY = ".pkey";
  private static final String DATALOCATION = ".datalocation";
  private static final String SCHEMALOCATION = ".schemalocation";
  private static final String COPYDATA = ".copydata";
  private static final String COPYDATA_DEFAULT = "false";
  private static final String DATAFORMAT_EXTENSION_NAME = ".dataformat.extension.name";
  private static final String OUTPUT = "output";

  static Properties properties = new Properties();
  static Properties jobProperties = new Properties();

  public static void main(String[] args) throws IOException, ConfigurationException {

    properties = CliOptions.parseArgs(MRCompactionRunner.class, args);

    File compactionConfigDir = new File(properties.getProperty(COMPACTION_CONFIG_DIR));
    File[] listOfFiles = compactionConfigDir.listFiles();
    if (listOfFiles == null || listOfFiles.length == 0) {
      System.err.println("No compaction configuration files found under " + compactionConfigDir);
      System.exit(1);
    }

    int numOfJobs = 0;
    for (File file : listOfFiles) {
      if (file.isFile() && !file.getName().startsWith(".")) {
        numOfJobs++;
      }
    }
    LOG.info("Found " + numOfJobs + " compaction tasks.");
    try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(properties.getProperty(TIMING_FILE, TIMING_FILE_DEFAULT)), Charset.forName("UTF-8")))) {

      for (File file : listOfFiles) {
        if (file.isFile() && !file.getName().startsWith(".")) {
          Configuration jobConfig = new PropertiesConfiguration(file.getAbsolutePath());
          jobProperties = ConfigurationConverter.getProperties(jobConfig);
          long startTime = System.nanoTime();
          compact();
          long endTime = System.nanoTime();
          long elapsedTime = endTime - startTime;
          double seconds = TimeUnit.NANOSECONDS.toSeconds(elapsedTime);
          pw.printf("%s: %f%n", file.getAbsolutePath(), seconds);
        }
      }
    }
  }

  private static void compact() throws IOException {

    SerialCompactor sc = new SerialCompactor.Builder().withSnapshot(buildSnapshotTable()).withDeltas(buildDeltaTables())
        .withOutputTableName(jobProperties.getProperty(OUTPUT + NAME))
        .withOutputDataLocationInHdfs(jobProperties.getProperty(OUTPUT + DATALOCATION)).build();
    sc.compact();
  }

  private static AvroExternalTable buildSnapshotTable() throws IOException {
    return buildAvroExternalTable(SNAPSHOT);
  }

  private static List<AvroExternalTable> buildDeltaTables() throws IOException {
    List<AvroExternalTable> deltas = new ArrayList<>();

    for (int i = 1;; i++) {
      String deltai = DELTA + "." + i;
      if (jobProperties.getProperty(deltai + DATALOCATION) == null) {
        break;
      }
      deltas.add(buildAvroExternalTable(deltai));
    }

    return deltas;
  }

  private static AvroExternalTable buildAvroExternalTable(String tableType) throws IOException {
    AvroExternalTable.Builder builder =
        new AvroExternalTable.Builder().withName(jobProperties.getProperty(tableType + NAME, ""))
            .withPrimaryKeys(jobProperties.getProperty(tableType + PKEY))
            .withSchemaLocation(jobProperties.getProperty(tableType + SCHEMALOCATION, ""))
            .withDataLocation(jobProperties.getProperty(tableType + DATALOCATION));

    if (Boolean.parseBoolean(jobProperties.getProperty(tableType + COPYDATA, COPYDATA_DEFAULT))) {
      builder = builder.withMoveDataToTmpHdfsDir(jobProperties.getProperty(tableType + DATAFORMAT_EXTENSION_NAME, ""));
    }

    return builder.build();
  }
}
