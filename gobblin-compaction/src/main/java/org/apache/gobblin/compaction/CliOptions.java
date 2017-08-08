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

package org.apache.gobblin.compaction;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.gobblin.util.JobConfigurationUtils;


/**
 * Utility class for parsing command line options for Gobblin compaction jobs.
 *
 * @author Lorand Bendig
 *
 */
public class CliOptions {

  private final static Option JOB_CONFIG_OPTION = Option.builder().argName("job configuration file")
      .desc("Gobblin compaction job configuration file").hasArgs().longOpt("jobconfig").build();
  private final static Option HELP_OPTION =
      Option.builder("h").argName("help").desc("Display usage information").longOpt("help").build();

  /**
   * Parse command line arguments and return a {@link java.util.Properties} object for the Gobblin job found.
   * @param caller Class of the calling main method. Used for error logs.
   * @param args Command line arguments.
   * @param conf Hadoop configuration object
   * @return Instance of {@link Properties} for the Gobblin job to run.
   * @throws IOException
   */
  public static Properties parseArgs(Class<?> caller, String[] args, Configuration conf) throws IOException {
    try {

      // Parse command-line options
      if (conf != null) {
        args = new GenericOptionsParser(conf, args).getCommandLine().getArgs();
      }
      CommandLine cmd = new DefaultParser().parse(options(), args);

      if (cmd.hasOption(HELP_OPTION.getOpt())) {
        printUsage(caller);
        System.exit(0);
      }

      String jobConfigLocation = JOB_CONFIG_OPTION.getLongOpt();
      if (!cmd.hasOption(jobConfigLocation)) {
        printUsage(caller);
        System.exit(1);
      }

      // Load job configuration properties
      Properties jobConfig;
      if (conf == null) {
        jobConfig = JobConfigurationUtils.fileToProperties(cmd.getOptionValue(jobConfigLocation));
      } else {
        jobConfig = JobConfigurationUtils.fileToProperties(cmd.getOptionValue(jobConfigLocation), conf);
        JobConfigurationUtils.putConfigurationIntoProperties(conf, jobConfig);
      }
      return jobConfig;
    } catch (ParseException | ConfigurationException e) {
      throw new IOException(e);
    }
  }

  public static Properties parseArgs(Class<?> caller, String[] args) throws IOException {
    return parseArgs(caller, args, null);
  }

  /**
   * Prints the usage of cli.
   * @param caller Class of the main method called. Used in printing the usage message.
   */
  public static void printUsage(Class<?> caller) {
    new HelpFormatter().printHelp(caller.getSimpleName(), options());
  }

  private static Options options() {
    Options options = new Options();
    options.addOption(JOB_CONFIG_OPTION);
    options.addOption(HELP_OPTION);
    return options;
  }

}
