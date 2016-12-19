/*
 * Copyright (C) 2016-2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.listeners;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.annotation.Alias;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.dataset.Dataset;
import gobblin.configuration.State;


public class SimpleCompactorCompletionListener implements CompactorCompletionListener {
  private static final Logger logger = LoggerFactory.getLogger (SimpleCompactorCompletionListener.class);

  private SimpleCompactorCompletionListener (State state) {
  }

  public void onCompactionCompletion (MRCompactor compactor) {
    logger.info(String.format("Compaction (started on : %s) is finished", compactor.getInitializeTime()));
    Set<Dataset> datasets = compactor.getDatasets();

    for (Dataset dataset: datasets) {
      if (dataset.state() != Dataset.DatasetState.COMPACTION_COMPLETE) {
        logger.error("Dataset " + dataset.getDatasetName() + " " + dataset.state().name());
      }
    }
  }

  @Alias("SimpleCompactorCompletionHook")
  public static class Factory implements CompactorCompletionListenerFactory {
    @Override public CompactorCompletionListener createCompactorCompactionListener (State state) {
      return new SimpleCompactorCompletionListener (state);
    }
  }
}
