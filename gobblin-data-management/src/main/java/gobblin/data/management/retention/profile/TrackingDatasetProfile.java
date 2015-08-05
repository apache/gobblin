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

package gobblin.data.management.retention.profile;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.dataset.TrackingDataset;
import gobblin.data.management.retention.version.finder.DateTimeDatasetVersionFinder;


/**
 * {@link gobblin.data.management.retention.dataset.finder.DatasetFinder} for tracking datasets.
 *
 * <p>
 *   Tracking datasets are datasets where each data point represents a timestamped action, and the records are
 *   organized in a time aware directory pattern (e.g. one directory per minute / hour / day).
 * </p>
 */
public class TrackingDatasetProfile extends ConfigurableGlobDatasetFinder {

  public TrackingDatasetProfile(FileSystem fs, Properties props)
      throws IOException {
    super(fs, props);
  }

  @Override
  public List<String> requiredProperties() {
    List<String> requiredProperties = super.requiredProperties();
    requiredProperties.add(DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY);
    return requiredProperties;
  }

  @Override
  public Dataset datasetAtPath(Path path)
      throws IOException {
    return new TrackingDataset(this.fs, this.props, path);
  }
}
