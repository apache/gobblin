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

package org.apache.gobblin.data.management.retention.version.finder;

import org.apache.gobblin.data.management.retention.version.DatasetVersion;
import org.apache.gobblin.data.management.retention.version.TimestampedDatasetVersion;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * @deprecated
 * See javadoc for {@link org.apache.gobblin.data.management.version.finder.UnixTimestampVersionFinder}.
 */
@Deprecated
public class UnixTimestampVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private final org.apache.gobblin.data.management.version.finder.UnixTimestampVersionFinder realVersionFinder;

  public UnixTimestampVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    this.realVersionFinder =
        new org.apache.gobblin.data.management.version.finder.UnixTimestampVersionFinder(fs, convertDeprecatedProperties(props));
  }

  @Override
  public Path globVersionPattern() {
    return this.realVersionFinder.globVersionPattern();
  }

  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    org.apache.gobblin.data.management.version.TimestampedDatasetVersion timestampedDatasetVersion =
        this.realVersionFinder.getDatasetVersion(pathRelativeToDatasetRoot, fullPath);
    if (timestampedDatasetVersion != null) {
      return new TimestampedDatasetVersion(timestampedDatasetVersion);
    }
    return null;
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  private static Properties convertDeprecatedProperties(Properties props) {
    return WatermarkDatasetVersionFinder.convertDeprecatedProperties(props);
  }
}
