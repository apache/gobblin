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

package org.apache.gobblin.data.management.copy;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Sets;

import org.apache.gobblin.data.management.dataset.DatasetUtils;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filters.AndPathFilter;
import org.apache.gobblin.util.filters.HiddenFilter;


/**
 * Used to find the path recursively from the root based on the {@link PathFilter} created in the {@link Properties}
 * @author mitu
 *
 */
public class RecursivePathFinder {

  private final Path rootPath;
  private final FileSystem fs;
  private final PathFilter pathFilter;
  private final boolean includeEmptyDirectories;

  public RecursivePathFinder(final FileSystem fs, Path rootPath, Properties properties) {
    this.rootPath = PathUtils.getPathWithoutSchemeAndAuthority(rootPath);
    this.fs = fs;

    this.pathFilter = DatasetUtils.instantiatePathFilter(properties);
    this.includeEmptyDirectories =
        Boolean.parseBoolean(properties.getProperty(CopyConfiguration.INCLUDE_EMPTY_DIRECTORIES));
  }

  public Set<FileStatus> getPaths(boolean skipHiddenPaths)
      throws IOException {

    if (!this.fs.exists(this.rootPath)) {
      return Sets.newHashSet();
    }
    PathFilter actualFilter =
        skipHiddenPaths ? new AndPathFilter(new HiddenFilter(), this.pathFilter) : this.pathFilter;
    List<FileStatus> files =
        FileListUtils.listFilesToCopyAtPath(this.fs, this.rootPath, actualFilter, includeEmptyDirectories);

    return Sets.newHashSet(files);
  }
}
