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

import gobblin.compaction.Compactor;
import gobblin.configuration.ConfigurationKeys;

import java.util.Properties;

import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;


/**
 * A class for launching a Gobblin MR job for compaction through Azkaban.
 *
 * @author ziliu
 */
public class AzkabanCompactionJobLauncher extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanCompactionJobLauncher.class);

  private final Properties properties;

  public AzkabanCompactionJobLauncher(String jobId, Properties props) {
    super(jobId, LOG);
    this.properties = new Properties();
    this.properties.putAll(props);
  }

  @Override
  public void run() throws Exception {
    Class<? extends Compactor> compactorClass = getCompactorClass();
    compactorClass.getDeclaredConstructor(Properties.class).newInstance(this.properties).compact();
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Compactor> getCompactorClass() throws ClassNotFoundException {
    String compactorClassName =
        this.properties.getProperty(ConfigurationKeys.COMPACTION_COMPACTOR_CLASS,
            ConfigurationKeys.DEFAULT_COMPACTION_COMPACTOR_CLASS);
    return (Class<? extends Compactor>) Class.forName(compactorClassName);
  }

}
