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

package gobblin.runtime.util;

import com.google.common.io.Closer;
import com.google.gson.stream.JsonWriter;
import com.typesafe.config.Config;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.StateStore;
import gobblin.runtime.JobState;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;
import gobblin.util.JobConfigurationUtils;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;


/**
 * A utility class for converting a {@link gobblin.runtime.JobState} object to a json-formatted document.
 *
 * @author Yinan Li
 */
public class JobStateToJsonConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobStateToJsonConverter.class);

  private static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

  private final StateStore<? extends JobState> jobStateStore;
  private final boolean keepConfig;

  @SuppressWarnings("unchecked")
  public JobStateToJsonConverter(Properties props, String storeUrl, boolean keepConfig) throws IOException {
    Configuration conf = new Configuration();
    JobConfigurationUtils.putPropertiesIntoConfiguration(props, conf);

    this.jobStateStore = (StateStore) createStateStore(ConfigUtils.propertiesToConfig(props));
    this.keepConfig = keepConfig;
  }


  protected DatasetStateStore createStateStore(Config jobConfig) throws IOException {
    boolean stateStoreEnabled = !jobConfig.hasPath(ConfigurationKeys.STATE_STORE_ENABLED)
        || jobConfig.getBoolean(ConfigurationKeys.STATE_STORE_ENABLED);

    String stateStoreType;

    if (!stateStoreEnabled) {
      stateStoreType = ConfigurationKeys.STATE_STORE_TYPE_NOOP;
    } else {
      stateStoreType = ConfigUtils.getString(jobConfig, ConfigurationKeys.STATE_STORE_TYPE_KEY,
          ConfigurationKeys.DEFAULT_STATE_STORE_TYPE);
    }

    ClassAliasResolver<DatasetStateStore.Factory> resolver =
        new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    try {
      DatasetStateStore.Factory stateStoreFactory =
          resolver.resolveClass(stateStoreType).newInstance();
      return stateStoreFactory.createStateStore(jobConfig);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
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

  @SuppressWarnings("all")
  public static void main(String[] args) throws Exception {
    Option sysConfigOption = Option.builder("sc").argName("system configuration file")
        .desc("Gobblin system configuration file").longOpt("sysconfig").hasArgs().build();
    Option storeUrlOption = Option.builder("u").argName("gobblin state store URL")
        .desc("Gobblin state store root path URL").longOpt("storeurl").hasArgs().required().build();
    Option jobNameOption = Option.builder("n").argName("gobblin job name").desc("Gobblin job name").longOpt("name")
        .hasArgs().required().build();
    Option jobIdOption =
        Option.builder("i").argName("gobblin job id").desc("Gobblin job id").longOpt("id").hasArgs().build();
    Option convertAllOption =
        Option.builder("a").desc("Whether to convert all past job states of the given job").longOpt("all").build();
    Option keepConfigOption =
        Option.builder("kc").desc("Whether to keep all configuration properties").longOpt("keepConfig").build();
    Option outputToFile =
        Option.builder("t").argName("output file name").desc("Output file name").longOpt("toFile").hasArgs().build();

    Options options = new Options();
    options.addOption(sysConfigOption);
    options.addOption(storeUrlOption);
    options.addOption(jobNameOption);
    options.addOption(jobIdOption);
    options.addOption(convertAllOption);
    options.addOption(keepConfigOption);
    options.addOption(outputToFile);

    CommandLine cmd = null;
    try {
      CommandLineParser parser = new DefaultParser();
      cmd = parser.parse(options, args);
    } catch (ParseException pe) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("JobStateToJsonConverter", options);
      System.exit(1);
    }

    Properties sysConfig = new Properties();
    if (cmd.hasOption(sysConfigOption.getLongOpt())) {
      sysConfig = JobConfigurationUtils.fileToProperties(cmd.getOptionValue(sysConfigOption.getLongOpt()));
    }

    JobStateToJsonConverter converter =
        new JobStateToJsonConverter(sysConfig, cmd.getOptionValue('u'), cmd.hasOption("kc"));
    StringWriter stringWriter = new StringWriter();
    if (cmd.hasOption('i')) {
      converter.convert(cmd.getOptionValue('n'), cmd.getOptionValue('i'), stringWriter);
    } else {
      if (cmd.hasOption('a')) {
        converter.convertAll(cmd.getOptionValue('n'), stringWriter);
      } else {
        converter.convert(cmd.getOptionValue('n'), stringWriter);
      }
    }

    if (cmd.hasOption('t')) {
      Closer closer = Closer.create();
      try {
        FileOutputStream fileOutputStream = closer.register(new FileOutputStream(cmd.getOptionValue('t')));
        OutputStreamWriter outputStreamWriter =
            closer.register(new OutputStreamWriter(fileOutputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
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