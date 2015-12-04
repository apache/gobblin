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

package gobblin.runtime.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import com.google.gson.stream.JsonWriter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.StateStore;
import gobblin.runtime.FsDatasetStateStore;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskState;


/**
 * A utility class for converting a {@link gobblin.runtime.JobState} object to a json-formatted document.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class JobStateToJsonConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobStateToJsonConverter.class);

  private static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

  private final String storeUrl;
  private final StateStore<? extends JobState> jobStateStore;
  private final boolean keepConfig;

  public JobStateToJsonConverter(String storeUrl, boolean keepConfig)
      throws IOException {
	  this.storeUrl = storeUrl;
    this.jobStateStore = new FsDatasetStateStore(storeUrl);
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
  @SuppressWarnings("unchecked")
  public void convert(String jobName, String jobId, Writer writer)
      throws IOException {
    List<? extends JobState> jobStates = this.jobStateStore.getAll(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX);
    if (jobStates.isEmpty()) {
      LOGGER.warn(String.format("No job state found for job with name %s and id %s", jobName, jobId));
      return;
    }

    JsonWriter jsonWriter = new JsonWriter(writer);
    jsonWriter.setIndent("\t");
    try {
      // There should be only a single job state
      writeJobState(jsonWriter, jobStates.get(0));
    } finally {
      jsonWriter.close();
    }
  }
  
  /**
   * Modify the configuration value of a given job instance.
   */
  public void modify(String jobName, String jobId, String key, String newValue) throws IOException {
    List<? extends JobState> jobStates = this.jobStateStore.getAll(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX);
    if (jobStates.isEmpty()) {
      LOGGER.warn(String.format("No job state found for job with name %s and id %s", jobName, jobId));
      return;
    }
    JobState jobState = jobStates.get(0);

    // Update the property in job state.
    jobState.setProp(key, newValue);
    
    // Update the property in task state.
    for (TaskState taskState: jobState.getTaskStates()) {
      taskState.setProp(key, newValue);
    }

    this.replaceStateStore(jobName, jobId, (JobState.DatasetState)jobState);
  }

  /**
   * Modify the configuration value of the most recent job instance.
   */
  public void modify(String jobName, String key, String newValue) throws IOException {
    this.modify(jobName, "current", key, newValue);
  }
  
  /**
   * Modify all past {@link JobState}s of the given job.
   */
  public void modifyAll(String jobName, String key, String newValue) throws IOException {
    List<? extends JobState> jobStates = this.jobStateStore.getAll(jobName);
    for (JobState jobState : jobStates) {
      LOGGER.info("Will modify job state " + jobState.getJobId());
      this.modify(jobName, jobState.getJobId(), key, newValue);
    }
  }

  /**
   * Replace the original state file with a new state.
   */
  private void replaceStateStore(String jobName, String jobId, JobState.DatasetState newState) throws IOException {
    Path storePath = new Path(storeUrl);
    Path tablePath = new Path(new Path(storePath.toUri().getPath(), jobName), jobId + JOB_STATE_STORE_TABLE_SUFFIX);
    FileSystem fs = storePath.getFileSystem(new Configuration());
    Path tmpTablePath = new Path(tablePath.getParent(), "original"+ tablePath.getName());
    if (!fs.rename(tablePath, tmpTablePath)) {
      throw new IOException("Failed to make a copy of the orginal state file " + tablePath);
    }
      
    try {
      ((FsDatasetStateStore)this.jobStateStore).put(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX, newState);
	    fs.delete(tmpTablePath, true);
	  } catch (IOException e) {
	    LOGGER.error("Failed to write new state to " + tablePath + ". Will recover the previous state file");
	    fs.rename(tmpTablePath, tablePath);
	    throw e;
	  }
  }

  /**
   * Convert the most recent {@link JobState} of the given job.
   *
   * @param jobName job name
   * @param writer {@link java.io.Writer} to write the json document
   */
  @SuppressWarnings("unchecked")
  public void convert(String jobName, Writer writer)
      throws IOException {
    convert(jobName, "current", writer);
  }

  /**
   * Convert all past {@link JobState}s of the given job.
   *
   * @param jobName job name
   * @param writer {@link java.io.Writer} to write the json document
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void convertAll(String jobName, Writer writer)
      throws IOException {
    List<? extends JobState> jobStates = this.jobStateStore.getAll(jobName);
    if (jobStates.isEmpty()) {
      LOGGER.warn(String.format("No job state found for job with name %s", jobName));
      return;
    }

    JsonWriter jsonWriter = new JsonWriter(writer);
    jsonWriter.setIndent("\t");
    try {
      writeJobStates(jsonWriter, jobStates);
    } finally {
      jsonWriter.close();
    }
  }

  /**
   * Write a single {@link JobState} to json document.
   *
   * @param jsonWriter {@link com.google.gson.stream.JsonWriter}
   * @param jobState {@link JobState} to write to json document
   * @throws IOException
   */
  private void writeJobState(JsonWriter jsonWriter, JobState jobState)
      throws IOException {
    jobState.toJson(jsonWriter, this.keepConfig);
  }

  /**
   * Write a list of {@link JobState}s to json document.
   *
   * @param jsonWriter {@link com.google.gson.stream.JsonWriter}
   * @param jobStates list of {@link JobState}s to write to json document
   * @throws IOException
   */
  private void writeJobStates(JsonWriter jsonWriter, List<? extends JobState> jobStates)
      throws IOException {
    jsonWriter.beginArray();
    for (JobState jobState : jobStates) {
      writeJobState(jsonWriter, jobState);
    }
    jsonWriter.endArray();
  }

  @SuppressWarnings("all")
  public static void main(String[] args)
      throws Exception {
    Option storeUrlOption = OptionBuilder
        .withArgName("gobblin state store URL")
        .withDescription("Gobblin state store root path URL")
        .withLongOpt("storeurl")
        .hasArgs()
        .isRequired()
        .create('u');
    Option jobNameOption = OptionBuilder
        .withArgName("gobblin job name")
        .withDescription("Gobblin job name")
        .withLongOpt("name")
        .hasArgs()
        .isRequired()
        .create('n');
    Option jobIdOption = OptionBuilder
        .withArgName("gobblin job id")
        .withDescription("Gobblin job id")
        .withLongOpt("id")
        .hasArgs()
        .create('i');
    Option convertAllOption = OptionBuilder
        .withDescription("Whether to convert all past job states of the given job. Will modify all past job states if used with option '-mc'.")
        .withLongOpt("all")
        .create('a');
    Option keepConfigOption = OptionBuilder
        .withDescription("Whether to keep all configuration properties")
        .withLongOpt("keepConfig")
        .create("kc");
    Option outputToFile = OptionBuilder
        .withArgName("output file name")
        .withDescription("Output file name")
        .withLongOpt("toFile")
        .hasArgs()
        .create("t");
    Option modifyPropertyOption = OptionBuilder
    		.withDescription("Modify a property in state store using KEY=NEWVALUE. If KEY does not exist, it will be added to the state store.")
            .withLongOpt("modifyProperty")
            .hasArgs()
            .create("mc");

    Options options = new Options();
    options.addOption(storeUrlOption);
    options.addOption(jobNameOption);
    options.addOption(jobIdOption);
    options.addOption(convertAllOption);
    options.addOption(keepConfigOption);
    options.addOption(outputToFile);
    options.addOption(modifyPropertyOption);

    CommandLine cmd = null;
    try {
      CommandLineParser parser = new BasicParser();
      cmd = parser.parse(options, args);
    } catch (ParseException pe) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("JobStateToJsonConverter", options);
      System.exit(1);
    }

    JobStateToJsonConverter converter = new JobStateToJsonConverter(cmd.getOptionValue('u'), cmd.hasOption("kc"));
    
    if (cmd.hasOption("mc")) {
    	String keyValuePair = cmd.getOptionValue("mc");
    	int separatorIndex = keyValuePair.indexOf('=');
    	if (separatorIndex <= 0) {
    		throw new RuntimeException("Option '-mc' must be followed by key value pair with the pattern Key=NewValue.");
    	} else {
    	  if (cmd.hasOption('i')) {
    		  converter.modify(cmd.getOptionValue('n'), cmd.getOptionValue('i'), keyValuePair.substring(0, separatorIndex), keyValuePair.substring(separatorIndex+1));
    	  } else if (cmd.hasOption('a')) {
    	    converter.modifyAll(cmd.getOptionValue('n'), keyValuePair.substring(0, separatorIndex), keyValuePair.substring(separatorIndex+1));
    	  } else {
          converter.modify(cmd.getOptionValue('n'), keyValuePair.substring(0, separatorIndex), keyValuePair.substring(separatorIndex+1));
    	  }
    	}
    }

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
