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

package gobblin.runtime.local;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import gobblin.runtime.JobException;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.app.ApplicationException;
import gobblin.runtime.app.ApplicationLauncher;
import gobblin.runtime.app.ServiceBasedAppLauncher;
import gobblin.runtime.cli.CliOptions;
import gobblin.runtime.listeners.JobListener;


/**
 * Launcher for creating a Gobblin job from command line or IDE.
 */
public class CliLocalJobLauncher implements ApplicationLauncher, JobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(CliLocalJobLauncher.class);

  private final Closer closer = Closer.create();

  private final ApplicationLauncher applicationLauncher;
  private final LocalJobLauncher localJobLauncher;

  private CliLocalJobLauncher(Properties properties) throws Exception {
    this.applicationLauncher = this.closer.register(new ServiceBasedAppLauncher(properties,
        properties.getProperty(ServiceBasedAppLauncher.APP_NAME, "CliLocalJob-" + UUID.randomUUID())));
    this.localJobLauncher = this.closer.register(new LocalJobLauncher(properties));
  }

  public void run() throws ApplicationException, JobException, IOException {
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
  }

  public static void main(String[] args) throws Exception {
    Properties jobProperties = CliOptions.parseArgs(CliLocalJobLauncher.class, args);

    LOG.debug(String.format("Running job with properties:%n%s", jobProperties));
    new CliLocalJobLauncher(jobProperties).run();
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
    this.localJobLauncher.launchJob(jobListener);
  }

  @Override
  public void cancelJob(@Nullable JobListener jobListener) throws JobException {
    this.localJobLauncher.cancelJob(jobListener);
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }
}
