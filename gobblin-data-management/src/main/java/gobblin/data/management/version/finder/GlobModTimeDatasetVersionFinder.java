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

package gobblin.data.management.version.finder;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import com.typesafe.config.Config;

import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * Finds {@link FileSystemDatasetVersion}s using a glob pattern. Uses Modification time as the version.
 */
public class GlobModTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private final Path globPattern;

  private static final String VERSION_FINDER_GLOB_PATTERN_KEY = "version.globPattern";

  public GlobModTimeDatasetVersionFinder(FileSystem fs, Config config) {
    this(fs, config.hasPath(VERSION_FINDER_GLOB_PATTERN_KEY) ? new Path(config.getString(VERSION_FINDER_GLOB_PATTERN_KEY)) : new Path("*"));
  }

  public GlobModTimeDatasetVersionFinder(FileSystem fs, Path globPattern) {
    super(fs);
    this.globPattern = globPattern;
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Path globVersionPattern() {
    return this.globPattern;
  }

  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    try {
      return new TimestampedDatasetVersion(new DateTime(fs.getFileStatus(fullPath).getModificationTime()), fullPath);
    } catch (IOException e) {
      return null;
    }
  }
}
