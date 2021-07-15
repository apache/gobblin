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

package org.apache.gobblin.runtime.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.FsStateStoreFactory;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.util.JobStateToJsonConverter;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobConfigurationUtils;


@Slf4j
@Alias(value = "job-state-store", description = "View or delete JobState in state store")
public class JobStateStoreCLI implements CliApplication {

  Option sysConfigOption = Option.builder("sc").argName("system configuration file")
      .desc("Gobblin system configuration file (required if no state store URL specified)").longOpt("sysconfig").hasArg().build();
  Option storeUrlOption = Option.builder("u").argName("gobblin state store URL")
      .desc("Gobblin state store root path URL (required if no sysconfig specified)").longOpt("storeurl").hasArg().build();
  Option jobNameOption = Option.builder("n").argName("gobblin job name").desc("Gobblin job name").longOpt("name")
      .hasArg().build();
  Option jobIdOption =
      Option.builder("i").argName("gobblin job id").desc("Gobblin job id").longOpt("id").hasArg().build();
  Option helpOption =
      Option.builder("h").argName("help").desc("Usage").longOpt("help").hasArg().build();
  Option deleteOption =
      Option.builder("d").argName("delete state").desc("Deletes a state from the state store with a job id")
          .longOpt("delete").build();
  Option bulkDeleteOption =
      Option.builder("bd").argName("bulk delete")
          .desc("Deletes states from the state store based on a file with job ids to delete, separated by newline")
          .longOpt("bulkDelete").hasArg().build();

  // For reading state store in json format
  Option getAsJsonOption =
      Option.builder("r").argName("read job state").desc("Converts a job state to json").longOpt("read-job-state").build();
  Option convertAllOption =
      Option.builder("a").desc("Whether to convert all past job states of the given job when viewing as json").longOpt("all").build();
  Option keepConfigOption =
      Option.builder("kc").desc("Whether to keep all configuration properties when viewing as json").longOpt("keepConfig").build();
  Option outputToFile =
      Option.builder("t").argName("output file name").desc("Output file name when viewing as json").longOpt("toFile").hasArg().build();

  private static final Logger LOGGER = LoggerFactory.getLogger(JobStateStoreCLI.class);
  private StateStore<? extends JobState> jobStateStore;

  CommandLine initializeOptions(String[] args) {
    Options options = new Options();
    options.addOption(sysConfigOption);
    options.addOption(storeUrlOption);
    options.addOption(jobNameOption);
    options.addOption(jobIdOption);
    options.addOption(deleteOption);
    options.addOption(getAsJsonOption);
    options.addOption(convertAllOption);
    options.addOption(keepConfigOption);
    options.addOption(outputToFile);
    options.addOption(bulkDeleteOption);

    CommandLine cmd = null;

    try {
      CommandLineParser parser = new DefaultParser();
      cmd = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
    } catch (ParseException pe) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("JobStateStoreCLI", options);
      throw new RuntimeException(pe);
    }

    if (!cmd.hasOption(sysConfigOption.getLongOpt()) && !cmd.hasOption(storeUrlOption.getLongOpt()) ){
      System.out.println("State store configuration or state store url options missing");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("JobStateStoreCLI", options);
      return null;
    }

    if (cmd.hasOption(getAsJsonOption.getOpt()) && !cmd.hasOption(jobNameOption.getOpt())) {
      System.out.println("Job name option missing for reading job states as json");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("JobStateStoreCLI", options);
      return null;
    }

    if (cmd.hasOption(deleteOption.getOpt()) && !cmd.hasOption(jobNameOption.getOpt())) {
      System.out.println("Job name option missing for delete job id");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("JobStateStoreCLI", options);
      return null;
    }

    if (cmd.hasOption(helpOption.getOpt())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("JobStateStoreCLI", options);
      return null;
    }

    return cmd;
  }


  @Override
  public void run(String[] args) throws Exception {
    CommandLine cmd = initializeOptions(args);
    if (cmd == null) {
      return; // incorrect args were called
    }

    Properties props = new Properties();

    if (cmd.hasOption(sysConfigOption.getOpt())) {
      props = JobConfigurationUtils.fileToProperties(cmd.getOptionValue(sysConfigOption.getOpt()));
    }

    String storeUrl = cmd.getOptionValue(storeUrlOption.getLongOpt());
    if (StringUtils.isNotBlank(storeUrl)) {
      props.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, storeUrl);
    }

    Config stateStoreConfig = ConfigUtils.propertiesToConfig(props);
    ClassAliasResolver<StateStore.Factory> resolver =
        new ClassAliasResolver<>(StateStore.Factory.class);
    StateStore.Factory stateStoreFactory;

    try {
      stateStoreFactory = resolver.resolveClass(ConfigUtils.getString(stateStoreConfig, ConfigurationKeys.STATE_STORE_TYPE_KEY,
          FsStateStoreFactory.class.getName())).newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    this.jobStateStore = stateStoreFactory.createStateStore(stateStoreConfig, JobState.class);

    if (cmd.hasOption(getAsJsonOption.getOpt())) {
      this.viewStateAsJson(cmd);
    } else if (cmd.hasOption(bulkDeleteOption.getOpt())) {
      this.deleteJobBulk(cmd.getOptionValue(bulkDeleteOption.getOpt()));
    } else if (cmd.hasOption(deleteOption.getOpt())) {
      this.deleteJob(cmd.getOptionValue(jobNameOption.getOpt()));
    }
  }

  private void deleteJobBulk(String path) throws IOException {
    Path filePath = new Path(path);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(filePath.toString()), Charset.forName("UTF-8")))) {
      String jobName;
      while ((jobName = br.readLine()) != null) {
        System.out.println("Deleting " + jobName);
        try {
          this.jobStateStore.delete(jobName);
        } catch (IOException e) {
          System.out.println("Could not delete job name: " + jobName + " due to " + e.getMessage());
        }
      }
    }
  }

  private void deleteJob(String jobName) {
    System.out.println("Deleting " + jobName);
    try {
      this.jobStateStore.delete(jobName);
    } catch (IOException e) {
      System.out.println("Could not delete job name: " + jobName + " due to " + e.getMessage());
    }
  }

  private void viewStateAsJson(CommandLine cmd) throws IOException {
    JobStateToJsonConverter converter = new JobStateToJsonConverter(this.jobStateStore, cmd.hasOption("kc"));
    converter.outputToJson(cmd);
  }
}
