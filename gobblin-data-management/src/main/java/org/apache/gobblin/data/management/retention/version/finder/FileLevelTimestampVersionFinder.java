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

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.data.management.retention.version.TimestampedDatasetVersion;
import org.apache.gobblin.data.management.retention.version.DatasetVersion;
import org.apache.gobblin.dataset.Dataset;


/**
 * @deprecated
 * See javadoc for {@link org.apache.gobblin.data.management.version.finder.FileLevelTimestampVersionFinder}.
 */
@Deprecated
public class FileLevelTimestampVersionFinder implements VersionFinder<TimestampedDatasetVersion> {

  private final org.apache.gobblin.data.management.version.finder.FileLevelTimestampVersionFinder realVersionFinder;
  public FileLevelTimestampVersionFinder(FileSystem fs, Properties props) {
    this.realVersionFinder =
        new org.apache.gobblin.data.management.version.finder.FileLevelTimestampVersionFinder(fs,props);
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Collection<TimestampedDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    return TimestampedDatasetVersion.convertFromGeneralVersion(this.realVersionFinder
        .findDatasetVersions(dataset));
  }
}
