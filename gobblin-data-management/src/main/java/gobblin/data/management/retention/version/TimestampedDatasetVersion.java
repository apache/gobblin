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

package gobblin.data.management.retention.version;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;


/**
 * @deprecated
 * Extends {@link gobblin.data.management.version.TimestampedDatasetVersion} and implements
 * {@link gobblin.data.management.retention.version.DatasetVersion}.
 */
@Deprecated
public class TimestampedDatasetVersion extends gobblin.data.management.version.TimestampedDatasetVersion implements
    DatasetVersion {

  public TimestampedDatasetVersion(DateTime version, Path path) {
    super(version, path);
  }

  public TimestampedDatasetVersion(gobblin.data.management.version.TimestampedDatasetVersion datasetVersion) {
    this(datasetVersion.getVersion(), datasetVersion.getPath());
  }

  @Override
  public Set<Path> getPathsToDelete() {
    return this.getPaths();
  }

  public static Collection<TimestampedDatasetVersion> convertFromGeneralVersion(
      Collection<gobblin.data.management.version.TimestampedDatasetVersion> realVersions) {
    List<TimestampedDatasetVersion> timestampedVersions = Lists.newArrayList();
    for (gobblin.data.management.version.TimestampedDatasetVersion realVersion : realVersions) {
      timestampedVersions.add(new TimestampedDatasetVersion(realVersion));
    }
    return timestampedVersions;
  }
}
