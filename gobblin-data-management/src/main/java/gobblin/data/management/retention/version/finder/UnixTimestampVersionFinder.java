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

import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * @deprecated
 * See javadoc for {@link gobblin.data.management.version.finder.UnixTimestampVersionFinder}.
 */
@Deprecated
public class UnixTimestampVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private final gobblin.data.management.version.finder.UnixTimestampVersionFinder realVersionFinder;

  public UnixTimestampVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    this.realVersionFinder = new gobblin.data.management.version.finder.UnixTimestampVersionFinder(fs, props);
  }

  @Override
  public Path globVersionPattern() {
    return this.realVersionFinder.globVersionPattern();
  }

  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    return new TimestampedDatasetVersion(this.realVersionFinder.getDatasetVersion(pathRelativeToDatasetRoot, fullPath));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }
}
