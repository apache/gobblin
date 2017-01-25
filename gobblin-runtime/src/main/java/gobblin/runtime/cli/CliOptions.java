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

package gobblin.runtime.cli;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;

import gobblin.util.JobConfigurationUtils;


/**
 * Utility class for parsing command line options for Gobblin cli jobs.
 */
public class CliOptions {

  public final static Option SYS_CONFIG_OPTION = Option.builder().argName("system configuration file")
      .desc("Gobblin system configuration file").hasArgs().longOpt("sysconfig").build();
  public final static Option JOB_CONFIG_OPTION = Option.builder().argName("job configuration file")
      .desc("Gobblin job configuration file").hasArgs().longOpt("jobconfig").build();
  public final static Option HELP_OPTION =
      Option.builder("h").argName("help").desc("Display usage information").longOpt("help").build();

  /**
   * Parse command line arguments and return a {@link java.util.Properties} object for the gobblin job found.
   * @param caller Class of the calling main method. Used for error logs.
   * @param args Command line arguments.
   * @return Instance of {@link Properties} for the Gobblin job to run.
   * @throws IOException
   */
  public static Properties parseArgs(Class<?> caller, String[] args) throws IOException {
    try {
      // Parse command-line options
      CommandLine cmd = new DefaultParser().parse(options(), args);

      if (cmd.hasOption(HELP_OPTION.getOpt())) {
        printUsage(caller);
        System.exit(0);
      }

      if (!cmd.hasOption(SYS_CONFIG_OPTION.getLongOpt()) || !cmd.hasOption(JOB_CONFIG_OPTION.getLongOpt())) {
        printUsage(caller);
        System.exit(1);
      }

      // Load system and job configuration properties
      Properties sysConfig = JobConfigurationUtils.fileToProperties(cmd.getOptionValue(SYS_CONFIG_OPTION.getLongOpt()));
      Properties jobConfig = JobConfigurationUtils.fileToProperties(cmd.getOptionValue(JOB_CONFIG_OPTION.getLongOpt()));

      return JobConfigurationUtils.combineSysAndJobProperties(sysConfig, jobConfig);
    } catch (ParseException | ConfigurationException e) {
      throw new IOException(e);
    }
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
    options.addOption(SYS_CONFIG_OPTION);
    options.addOption(JOB_CONFIG_OPTION);
    options.addOption(HELP_OPTION);
    return options;
  }

}
