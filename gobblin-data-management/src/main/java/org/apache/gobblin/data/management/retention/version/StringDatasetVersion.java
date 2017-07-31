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

package org.apache.gobblin.data.management.retention.version;

import java.util.Set;

import org.apache.hadoop.fs.Path;


/**
 * @deprecated
 * Dataset version extends {@link org.apache.gobblin.data.management.version.StringDatasetVersion} and implements
 * {@link org.apache.gobblin.data.management.retention.version.DatasetVersion}.
 */
@Deprecated
public class StringDatasetVersion extends org.apache.gobblin.data.management.version.StringDatasetVersion implements
    DatasetVersion {

  public StringDatasetVersion(String version, Path path) {
    super(version, path);
  }

  public StringDatasetVersion(org.apache.gobblin.data.management.version.StringDatasetVersion datasetVersion) {
    this(datasetVersion.getVersion(), datasetVersion.getPath());
  }

  @Override
  public Set<Path> getPathsToDelete() {
    return this.getPaths();
  }
}
