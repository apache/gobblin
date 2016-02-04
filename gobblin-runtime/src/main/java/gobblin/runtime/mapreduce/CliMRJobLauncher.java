/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.runtime.JobLauncher;
import gobblin.runtime.cli.CliOptions;


/**
 * A utility class for launching a Gobblin Hadoop MR job through the command line.
 *
 * @author Yinan Li
 */
public class CliMRJobLauncher extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(CliMRJobLauncher.class);
  
  private final Properties jobProperties;

  public CliMRJobLauncher(Properties jobProperties) throws Exception {
    this.jobProperties = jobProperties;
  }

  @Override
  public int run(String[] args) throws Exception {

    try (JobLauncher launcher = new MRJobLauncher(jobProperties, getConf())) {
      launcher.launchJob(null);
    } catch (Exception e) {
      LOG.error("Failed to launch the job!", e);
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

    Properties jobProperties = CliOptions.parseArgs(CliMRJobLauncher.class, genericCmdLineOpts);
    
    // Launch and run the job
    System.exit(ToolRunner.run(conf, new CliMRJobLauncher(jobProperties), args));
  }
  
}
