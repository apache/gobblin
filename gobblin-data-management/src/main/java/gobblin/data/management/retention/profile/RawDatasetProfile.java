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

package gobblin.data.management.retention.profile;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.annotation.Alpha;
import gobblin.dataset.Dataset;
import gobblin.data.management.retention.dataset.RawDataset;
import gobblin.data.management.retention.version.DatasetVersion;


/**
 * {@link gobblin.data.management.retention.dataset.finder.DatasetFinder} for raw datasets.
 *
 * <p>
 *   A raw dataset is a dataset with a corresponding refined dataset.
 * </p>
 */
@Alpha
public class RawDatasetProfile<T extends DatasetVersion> extends ConfigurableGlobDatasetFinder {

  public RawDatasetProfile(FileSystem fs, Properties props) throws IOException {
    super(fs, props);
  }

  @Override
  public List<String> requiredProperties() {
    List<String> requiredProperties = super.requiredProperties();
    requiredProperties.add(RawDataset.DATASET_CLASS);
    requiredProperties.add(RawDataset.DATASET_RETENTION_POLICY_CLASS);
    return requiredProperties;
  }

  @Override
  public Dataset datasetAtPath(Path path) throws IOException {
    return new RawDataset(this.fs, this.props, path);
  }
}
