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
package gobblin.data.management.retention.version.finder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.data.management.retention.version.TimestampedDatasetVersion;
import gobblin.data.management.retention.version.DatasetVersion;


/**
 * @deprecated
 * See javadoc for {@link gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder}.
 */
@Deprecated
public class GlobModTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private final gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder realVersionFinder;
  public GlobModTimeDatasetVersionFinder(FileSystem fs, Path globPattern) {
    super(fs);
    this.realVersionFinder =
        new gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder(fs, globPattern);
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Path globVersionPattern() {
    return this.globVersionPattern();
  }

  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    return new TimestampedDatasetVersion(this.realVersionFinder.getDatasetVersion(pathRelativeToDatasetRoot, fullPath));
  }
}
