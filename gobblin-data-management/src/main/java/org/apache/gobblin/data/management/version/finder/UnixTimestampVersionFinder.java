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

package org.apache.gobblin.data.management.version.finder;

import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.data.management.version.StringDatasetVersion;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link VersionFinder} that generates {@link TimestampedDatasetVersion} from a unix timestamp
 * in the name of each version path.
 *
 * <p>
 *   The timestamp will be determined using a regex specified in the configuration key
 *   gobblin.retention.watermakr.regex . This class will attempt to interpret the 1st capture group in the regex as
 *   a unix timestamp. If no regex is provided, then the class will attempt to interpret the entire name of the
 *   version path as a unix timestamp.
 * </p>
 */
public class UnixTimestampVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnixTimestampVersionFinder.class);

  private final WatermarkDatasetVersionFinder embeddedFinder;

  public UnixTimestampVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    this.embeddedFinder = new WatermarkDatasetVersionFinder(fs, props);
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Path globVersionPattern() {
    return this.embeddedFinder.globVersionPattern();
  }

  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    StringDatasetVersion version = this.embeddedFinder.getDatasetVersion(pathRelativeToDatasetRoot, fullPath);
    if (version == null) {
      // This means that the embedded finder could not parse a version.
      return null;
    }
    try {
      Long timestamp = Long.parseLong(version.getVersion());
      return new TimestampedDatasetVersion(new DateTime(timestamp), fullPath);
    } catch (NumberFormatException nfe) {
      LOGGER.warn(String.format("Could not parse long from dataset version %s. Skipping.", pathRelativeToDatasetRoot));
      return null;
    } catch (IllegalArgumentException iae) {
      LOGGER.warn(String.format("Could not parse unix datetime for dataset version %s. Skipping.",
          pathRelativeToDatasetRoot));
      return null;
    }
  }
}
