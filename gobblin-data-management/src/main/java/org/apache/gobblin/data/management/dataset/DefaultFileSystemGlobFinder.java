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
package org.apache.gobblin.data.management.dataset;

import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Properties;

/**
 * A subclass of {@link ConfigurableGlobDatasetFinder} which find all the {@link FileSystemDataset}
 * that matches a given glob pattern.
 */
public class DefaultFileSystemGlobFinder extends ConfigurableGlobDatasetFinder<FileSystemDataset> {
  public DefaultFileSystemGlobFinder(FileSystem fs, Properties properties) throws IOException {
    super(fs, properties);
  }

  public FileSystemDataset datasetAtPath(final Path path) throws IOException {
    return new FileSystemDataset() {
      @Override
      public Path datasetRoot() {
        return path;
      }

      @Override
      public String datasetURN() {
        return path.toString();
      }
    };
  }
}
