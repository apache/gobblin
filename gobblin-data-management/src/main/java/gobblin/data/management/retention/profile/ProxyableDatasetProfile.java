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

package gobblin.data.management.retention.profile;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.retention.DatasetCleaner;
import gobblin.dataset.Dataset;
import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.util.ProxiedFileSystemCache;
import gobblin.util.RateControlledFileSystem;


/**
 * A wrapper of {@link gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder} that looks for
 * {@link gobblin.data.management.retention.dataset.Dataset}s with {@link org.apache.hadoop.fs.FileSystem}s
 * {@link gobblin.data.management.retention.dataset.CleanableDataset}s with {@link org.apache.hadoop.fs.FileSystem}s
 * proxied as the owner of each dataset.
 */
public class ProxyableDatasetProfile extends ConfigurableGlobDatasetFinder {

  public ProxyableDatasetProfile(FileSystem fs, Properties props) {
    super(fs, props);
  }

  @Override
  public Dataset datasetAtPath(Path path) throws IOException {
    return new ConfigurableCleanableDataset<DatasetVersion>(this.getFsForDataset(path), this.props, path);
  }

  public FileSystem getFsForDataset(Path path) throws IOException {
    Preconditions.checkArgument(this.props.containsKey(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS));
    Preconditions.checkArgument(this.props.containsKey(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION));
    FileSystem proxiedFileSystem = this.fs;
    try {
      proxiedFileSystem = ProxiedFileSystemCache.getProxiedFileSystemUsingKeytab(this.fs.getFileStatus(path).getOwner(),
          this.props.getProperty(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS),
          new Path(this.props.getProperty(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION)), this.fs.getUri(),
          this.fs.getConf());
    } catch (ExecutionException e) {
      throw new IOException("Cannot get proxied filesystem at Path: " + path, e);
    }

    if (this.props.contains(DatasetCleaner.DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT)) {
      return new RateControlledFileSystem(proxiedFileSystem,
          Long.parseLong(this.props.getProperty(DatasetCleaner.DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT)));
    }
    return proxiedFileSystem;
  }
}
