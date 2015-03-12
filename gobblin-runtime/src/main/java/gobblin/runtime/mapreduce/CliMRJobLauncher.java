/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.mapreduce;

import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import gobblin.runtime.JobException;


/**
 * A utility class for launching a Gobblin Hadoop MR job through the command line.
 *
 * @author ynli
 */
public class CliMRJobLauncher extends Configured implements Tool {

  private final Properties sysConfig;
  private final Properties jobConfig;

  public CliMRJobLauncher(Properties sysConfig, Properties jobConfig)
      throws Exception {
    this.sysConfig = sysConfig;
    this.jobConfig = jobConfig;
  }

  @Override
  public int run(String[] args)
      throws Exception {
    final Properties jobProps = new Properties();
    // First load system configuration properties
    jobProps.putAll(this.sysConfig);
    // Then load job configuration properties that might overwrite system configuration properties
    jobProps.putAll(this.jobConfig);

    try {
      new MRJobLauncher(this.sysConfig, getConf()).launchJob(jobProps, null);
    } catch (JobException je) {
      System.err.println("Failed to launch the job due to the following exception:");
      System.err.println(je.toString());
      return 1;
    }

    return 0;
  }

  /**
   * Print usage information.
   *
   * @param options command-line options
   */
  public static void printUsage(Options options) {
    new HelpFormatter().printHelp(CliMRJobLauncher.class.getSimpleName(), options);
  }

  public static void main(String[] args)
      throws Exception {
    
    Configuration conf = new Configuration();
    // Parse generic options
    String[] genericCmdLineOpts = new GenericOptionsParser(conf, args).getCommandLine().getArgs();
    
    // Build command-line options
    Option sysConfigOption = OptionBuilder
        .withArgName("system configuration file")
        .withDescription("Gobblin system configuration file")
        .hasArgs()
        .withLongOpt("sysconfig")
        .create();
    Option jobConfigOption = OptionBuilder
        .withArgName("job configuration file")
        .withDescription("Gobblin job configuration file")
        .hasArgs()
        .withLongOpt("jobconfig")
        .create();
    Option helpOption = OptionBuilder.withArgName("help")
        .withDescription("Display usage information")
        .withLongOpt("help")
        .create('h');

    Options options = new Options();
    options.addOption(sysConfigOption);
    options.addOption(jobConfigOption);
    options.addOption(helpOption);

    // Parse command-line options
    CommandLine cmd = new BasicParser().parse(options, genericCmdLineOpts);

    if (cmd.hasOption('h')) {
      printUsage(options);
      System.exit(0);
    }

    if (!cmd.hasOption("sysconfig") || !cmd.hasOption("jobconfig")) {
      printUsage(options);
      System.exit(1);
    }

    // Load system and job configuration properties
    Properties sysConfig =
        ConfigurationConverter.getProperties(new PropertiesConfiguration(cmd.getOptionValue("sysconfig")));
    Properties jobConfig =
        ConfigurationConverter.getProperties(new PropertiesConfiguration(cmd.getOptionValue("jobconfig")));

    // Launch and run the job
    System.exit(ToolRunner.run(conf, new CliMRJobLauncher(sysConfig, jobConfig), args));
  }
}
