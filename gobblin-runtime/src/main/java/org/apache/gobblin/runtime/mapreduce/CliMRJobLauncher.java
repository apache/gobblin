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

package org.apache.gobblin.runtime.mapreduce;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.io.Closer;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.cli.CliOptions;
import org.apache.gobblin.runtime.listeners.JobListener;


/**
 * A utility class for launching a Gobblin Hadoop MR job through the command line.
 *
 * @author Yinan Li
 */
@Slf4j
public class CliMRJobLauncher extends Configured implements ApplicationLauncher, JobLauncher, Tool {

  private final Closer closer = Closer.create();

  private final ApplicationLauncher applicationLauncher;
  private final MRJobLauncher mrJobLauncher;

  public CliMRJobLauncher(Configuration conf, Properties jobProperties) throws Exception {
    log.debug("Configuration: {}", conf);
    log.debug("Job properties: {}", jobProperties);
    setConf(conf);
    this.applicationLauncher = this.closer.register(new ServiceBasedAppLauncher(jobProperties,
        jobProperties.getProperty(ServiceBasedAppLauncher.APP_NAME, "CliMRJob-" + UUID.randomUUID())));
    this.mrJobLauncher = this.closer.register(new MRJobLauncher(jobProperties, getConf(), null));
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      start();
      launchJob(null);
    } finally {
      try {
        stop();
      } finally {
        close();
      }
    }
    return 0;
  }

  @Override
  public void start() throws ApplicationException {
    this.applicationLauncher.start();
  }

  @Override
  public void stop() throws ApplicationException {
    this.applicationLauncher.stop();
  }

  @Override
  public void launchJob(@Nullable JobListener jobListener) throws JobException {
    this.mrJobLauncher.launchJob(jobListener);
  }

  @Override
  public void cancelJob(@Nullable JobListener jobListener) throws JobException {
    this.mrJobLauncher.cancelJob(jobListener);
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    // Parse generic options
    String[] genericCmdLineOpts = new GenericOptionsParser(conf, args).getCommandLine().getArgs();

    Properties jobProperties = CliOptions.parseArgs(CliMRJobLauncher.class, genericCmdLineOpts);

    // Launch and run the job
    System.exit(ToolRunner.run(new CliMRJobLauncher(conf, jobProperties), args));
  }
}
