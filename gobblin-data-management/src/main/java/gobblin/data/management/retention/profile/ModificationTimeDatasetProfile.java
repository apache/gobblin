/*
 * Copyright (C) 2015-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.retention.profile;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.dataset.Dataset;
import gobblin.data.management.retention.dataset.ModificationTimeDataset;


/**
 * {@link gobblin.dataset.DatasetsFinder} for {@link ModificationTimeDataset}s.
 *
 * Modification time datasets will be cleaned by the modification timestamps of the datasets that match
 * 'gobblin.retention.dataset.pattern'.
 */
public class ModificationTimeDatasetProfile extends ConfigurableGlobDatasetFinder<Dataset> {
  public ModificationTimeDatasetProfile(FileSystem fs, Properties props) {
    super(fs, props);
  }

  @Override
  public Dataset datasetAtPath(Path path) throws IOException {
    return new ModificationTimeDataset(this.fs, this.props, path);
  }
}
