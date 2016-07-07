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
package gobblin.data.management.conversion.hive.source;

import java.util.List;

import gobblin.configuration.SourceState;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDatasetFinder;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.source.workunit.WorkUnit;

/**
 * An extension to {@link HiveSource} that is used for Avro to ORC conversion jobs.
 */
public class HiveAvroToOrcSource extends HiveSource {

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    if (!state.contains(HIVE_SOURCE_DATASET_FINDER_CLASS_KEY)) {
      state.setProp(HIVE_SOURCE_DATASET_FINDER_CLASS_KEY, ConvertibleHiveDatasetFinder.class.getName());
    }
    if (!state.contains(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY)) {
      state.setProp(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, "hive.conversion.avro");
    }

    return super.getWorkunits(state);
  }

}
