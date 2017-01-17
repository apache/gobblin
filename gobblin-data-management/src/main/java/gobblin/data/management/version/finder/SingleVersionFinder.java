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
import java.util.Collection;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.hadoop.fs.FileSystem;

import lombok.Getter;

import gobblin.dataset.Dataset;
import gobblin.dataset.FileSystemDataset;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.FileStatusDatasetVersion;
import gobblin.data.management.version.StringDatasetVersion;


/**
 * Implementation of {@link VersionFinder} that uses a {@link StringDatasetVersion} and simply creates a single
 * {@link StringDatasetVersion} for the given {@link FileSystemDataset}.
 */
public class SingleVersionFinder implements VersionFinder<FileStatusDatasetVersion> {

  @Getter
  private FileSystem fs;

  public SingleVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  public SingleVersionFinder(FileSystem fs, Config config) {
    this.fs = fs;
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return StringDatasetVersion.class;
  }

  @Override
  public Collection<FileStatusDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    return Lists.newArrayList(new FileStatusDatasetVersion(this.fs.getFileStatus(((FileSystemDataset) dataset)
        .datasetRoot())));
  }
}
