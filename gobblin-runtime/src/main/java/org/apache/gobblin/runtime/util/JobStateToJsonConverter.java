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

package org.apache.gobblin.runtime.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import com.google.gson.stream.JsonWriter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobState;


/**
 * A utility class for converting a {@link org.apache.gobblin.runtime.JobState} object to a json-formatted document.
 *
 * @author Yinan Li
 */
@Slf4j
public class JobStateToJsonConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobStateToJsonConverter.class);

  private static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

  private final StateStore<? extends JobState> jobStateStore;
  private final boolean keepConfig;

  public JobStateToJsonConverter(StateStore stateStore, boolean keepConfig) {
    this.keepConfig = keepConfig;
    this.jobStateStore = stateStore;
  }

  // Constructor for backwards compatibility
  public JobStateToJsonConverter(Properties props, String storeUrl, boolean keepConfig) throws IOException {
    Configuration conf = new Configuration();
    JobConfigurationUtils.putPropertiesIntoConfiguration(props, conf);

    if (StringUtils.isNotBlank(storeUrl)) {
      props.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, storeUrl);
    }

    this.jobStateStore = (StateStore) DatasetStateStore.buildDatasetStateStore(ConfigUtils.propertiesToConfig(props));
    this.keepConfig = keepConfig;
  }

  /**
   * Convert a single {@link JobState} of the given job instance.
   *
   * @param jobName job name
   * @param jobId job ID
   * @param writer {@link java.io.Writer} to write the json document
   * @throws IOException
   */
  public void convert(String jobName, String jobId, Writer writer) throws IOException {
    List<? extends JobState> jobStates = this.jobStateStore.getAll(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX);
    if (jobStates.isEmpty()) {
      LOGGER.warn(String.format("No job state found for job with name %s and id %s", jobName, jobId));
      return;
    }

    try (JsonWriter jsonWriter = new JsonWriter(writer)) {
      jsonWriter.setIndent("\t");
      // There should be only a single job state
      writeJobState(jsonWriter, jobStates.get(0));
    }
  }

  /**
   * Convert the most recent {@link JobState} of the given job.
   *
   * @param jobName job name
   * @param writer {@link java.io.Writer} to write the json document
   */
  public void convert(String jobName, Writer writer) throws IOException {
    convert(jobName, "current", writer);
  }

  /**
   * Convert all past {@link JobState}s of the given job.
   *
   * @param jobName job name
   * @param writer {@link java.io.Writer} to write the json document
   * @throws IOException
   */
  public void convertAll(String jobName, Writer writer) throws IOException {
    List<? extends JobState> jobStates = this.jobStateStore.getAll(jobName);
    if (jobStates.isEmpty()) {
      LOGGER.warn(String.format("No job state found for job with name %s", jobName));
      return;
    }

    try (JsonWriter jsonWriter = new JsonWriter(writer)) {
      jsonWriter.setIndent("\t");
      writeJobStates(jsonWriter, jobStates);
    }
  }

  /**
   * Write a single {@link JobState} to json document.
   *
   * @param jsonWriter {@link com.google.gson.stream.JsonWriter}
   * @param jobState {@link JobState} to write to json document
   * @throws IOException
   */
  private void writeJobState(JsonWriter jsonWriter, JobState jobState) throws IOException {
    jobState.toJson(jsonWriter, this.keepConfig);
  }

  /**
   * Write a list of {@link JobState}s to json document.
   *
   * @param jsonWriter {@link com.google.gson.stream.JsonWriter}
   * @param jobStates list of {@link JobState}s to write to json document
   * @throws IOException
   */
  private void writeJobStates(JsonWriter jsonWriter, List<? extends JobState> jobStates) throws IOException {
    jsonWriter.beginArray();
    for (JobState jobState : jobStates) {
      writeJobState(jsonWriter, jobState);
    }
    jsonWriter.endArray();
  }

  public void outputToJson(CommandLine cmd) throws IOException {
    StringWriter stringWriter = new StringWriter();
    if (cmd.hasOption('i')) {
      this.convert(cmd.getOptionValue('n'), cmd.getOptionValue('i'), stringWriter);
    } else {
      if (cmd.hasOption('a')) {
        this.convertAll(cmd.getOptionValue('n'), stringWriter);
      } else {
        this.convert(cmd.getOptionValue('n'), stringWriter);
      }
    }

    if (cmd.hasOption('t')) {
      Closer closer = Closer.create();
      try {
        FileOutputStream fileOutputStream = closer.register(new FileOutputStream(cmd.getOptionValue('t')));
        OutputStreamWriter outputStreamWriter = closer.register(new OutputStreamWriter(fileOutputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
        BufferedWriter bufferedWriter = closer.register(new BufferedWriter(outputStreamWriter));
        bufferedWriter.write(stringWriter.toString());
      } catch (Throwable t) {
        throw closer.rethrow(t);
      } finally {
        closer.close();
      }
    } else {
      System.out.println(stringWriter.toString());
    }
  }
}
