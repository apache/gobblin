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

import com.typesafe.config.Config;

import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;


/**
 * @deprecated
 * See javadoc for {@link gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder}.
 */
@Deprecated
public class GlobModTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private final gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder realVersionFinder;
  private static final String VERSION_FINDER_GLOB_PATTERN_KEY = "gobblin.retention.version.finder.pattern";

  public GlobModTimeDatasetVersionFinder(FileSystem fs, Config config) {
    this(fs, config.hasPath(VERSION_FINDER_GLOB_PATTERN_KEY) ? new Path(config.getString(VERSION_FINDER_GLOB_PATTERN_KEY)) : new Path("*"));
  }

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
    return this.realVersionFinder.globVersionPattern();
  }

  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    gobblin.data.management.version.TimestampedDatasetVersion timestampedDatasetVersion =
        this.realVersionFinder.getDatasetVersion(pathRelativeToDatasetRoot, fullPath);
    if (timestampedDatasetVersion != null) {
      return new TimestampedDatasetVersion(timestampedDatasetVersion);
    }
    return null;
  }
}
