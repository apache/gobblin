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

package gobblin.azkaban;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;

import gobblin.metastore.util.StateStoreCleaner;


/**
 * A utility class for running the {@link StateStoreCleaner} on Azkaban.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class AzkabanStateStoreCleanerJob extends AbstractJob {

  private static final Logger LOGGER = Logger.getLogger(AzkabanStateStoreCleanerJob.class);

  private final StateStoreCleaner stateStoreCleaner;

  public AzkabanStateStoreCleanerJob(String jobId, Properties props) throws IOException {
    super(jobId, LOGGER);
    this.stateStoreCleaner = new StateStoreCleaner(props);
  }

  @Override
  public void run()
      throws Exception {
    this.stateStoreCleaner.run();
  }
}
