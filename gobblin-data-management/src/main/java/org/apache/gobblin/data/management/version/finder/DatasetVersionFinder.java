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

import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.dataset.FileSystemDataset;


/**
 * Class to find {@link FileSystemDataset} versions in the file system.
 *
 * Concrete subclasses should implement a ({@link org.apache.hadoop.fs.FileSystem}, {@link java.util.Properties})
 * constructor to be instantiated.
 *
 * Provides a callback with just the path of the version {@link DatasetVersionFinder#getDatasetVersion(Path, Path)}.
 * Use {@link AbstractDatasetVersionFinder#getDatasetVersion(Path, FileStatus)} if you need a callback with {@link FileStatus}
 * of the version.
 *
 * @param <T> Type of {@link gobblin.data.management.version.FileSystemDatasetVersion} expected from this class.
 */
public abstract class DatasetVersionFinder<T extends FileSystemDatasetVersion> extends AbstractDatasetVersionFinder<T>
    implements VersionFinder<T> {

  public DatasetVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
  }

  public DatasetVersionFinder(FileSystem fs) {
    this(fs, new Properties());
  }

  @Override
  public T getDatasetVersion(Path pathRelativeToDatasetRoot, FileStatus versionFileStatus) {
    return getDatasetVersion(pathRelativeToDatasetRoot, versionFileStatus.getPath());
  }

  /**
   * Parse {@link gobblin.data.management.version.DatasetVersion} from the path of a dataset version.
   * @param pathRelativeToDatasetRoot {@link org.apache.hadoop.fs.Path} of dataset version relative to dataset root.
   * @param fullPath full {@link org.apache.hadoop.fs.Path} of the dataset version.
   * @return {@link gobblin.data.management.version.DatasetVersion} for that path.
   */
  public abstract T getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath);

}
