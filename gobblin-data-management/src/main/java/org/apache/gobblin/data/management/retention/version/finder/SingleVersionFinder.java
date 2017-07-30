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

package gobblin.data.management.retention.version.finder;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;

import lombok.Getter;

import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.FileStatusDatasetVersion;
import gobblin.dataset.Dataset;
import gobblin.dataset.FileSystemDataset;


/**
 * @deprecated
 * See javadoc for {@link gobblin.data.management.version.finder.SingleVersionFinder}.
 */
@Deprecated
public class SingleVersionFinder implements VersionFinder<FileStatusDatasetVersion> {

  @Getter
  private FileSystem fs;

  public SingleVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return FileStatusDatasetVersion.class;
  }

  @Override
  public Collection<FileStatusDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    return Lists.newArrayList(new FileStatusDatasetVersion(this.fs.getFileStatus(((FileSystemDataset) dataset)
        .datasetRoot())));
  }
}
