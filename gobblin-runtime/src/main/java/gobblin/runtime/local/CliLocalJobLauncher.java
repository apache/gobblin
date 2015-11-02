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

package gobblin.runtime.local;

import gobblin.runtime.cli.CliOptions;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Launcher for creating a Gobblin job from command line or IDE.
 */
public class CliLocalJobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(CliLocalJobLauncher.class);

  public static void main(String[] args) throws Exception {

    try {

      Properties jobProperties = CliOptions.parseArgs(CliLocalJobLauncher.class, args);
      LOG.debug(String.format("Running job with properties:\n%s", jobProperties));
      new LocalJobLauncher(jobProperties).launchJob(null);

    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

}
