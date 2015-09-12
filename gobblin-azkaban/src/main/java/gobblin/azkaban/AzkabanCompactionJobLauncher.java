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

import java.io.IOException;
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

  private static final String COMPACTION_COMPACTOR_CLASS = "compaction.compactor.class";
  private static final String DEFAULT_COMPACTION_COMPACTOR_CLASS = "gobblin.compaction.mapreduce.MRCompactor";

  private final Properties properties;
  private final Compactor compactor;

  public AzkabanCompactionJobLauncher(String jobId, Properties props) {
    super(jobId, LOG);
    this.properties = new Properties();
    this.properties.putAll(props);
    this.compactor = getCompactor();
  }

  private Compactor getCompactor() {
    try {
      Class<? extends Compactor> compactorClass = getCompactorClass();
      Compactor compactor = compactorClass.getDeclaredConstructor(Properties.class).newInstance(this.properties);
      return compactor;
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate compactor", e);
    }
  }

  @Override
  public void run() throws Exception {

    this.compactor.compact();
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Compactor> getCompactorClass() throws ClassNotFoundException {
    String compactorClassName =
        this.properties.getProperty(COMPACTION_COMPACTOR_CLASS, DEFAULT_COMPACTION_COMPACTOR_CLASS);
    return (Class<? extends Compactor>) Class.forName(compactorClassName);
  }

  @Override
  public void cancel() throws IOException {
    this.compactor.cancel();
  }

}
