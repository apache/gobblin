/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.data.management.version.finder;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * Finds {@link FileSystemDatasetVersion}s using a glob pattern. Uses Modification time as the version.
 */
public class GlobModTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private final Path globPattern;

  private static final String VERSION_FINDER_GLOB_PATTERN_KEY = "version.globPattern";

  public GlobModTimeDatasetVersionFinder(FileSystem fs, Config config) {
    this(fs, config.hasPath(VERSION_FINDER_GLOB_PATTERN_KEY)
        ? new Path(config.getString(VERSION_FINDER_GLOB_PATTERN_KEY)) : new Path("*"));
  }

  public GlobModTimeDatasetVersionFinder(FileSystem fs, Properties props) {
    this(fs, ConfigFactory.parseProperties(props));
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
      return new TimestampedDatasetVersion(new DateTime(this.fs.getFileStatus(fullPath).getModificationTime()),
          fullPath);
    } catch (IOException e) {
      return null;
    }
  }
}
