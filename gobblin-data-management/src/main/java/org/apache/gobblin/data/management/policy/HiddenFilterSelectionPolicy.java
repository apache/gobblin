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

package org.apache.gobblin.data.management.policy;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.util.ConfigUtils;


/*
 * Select dataset versions that pass the hidden path filter i.e. accept paths that do not have sub-dirs whose names start with "." or "_".
 */
public class HiddenFilterSelectionPolicy implements VersionSelectionPolicy<FileSystemDatasetVersion> {
  public static final String HIDDEN_FILTER_HIDDEN_FILE_PREFIX_KEY = "selection.hiddenFilter.hiddenFilePrefix";
  private static final String[] DEFAULT_HIDDEN_FILE_PREFIXES = {".", "_"};
  private List<String> hiddenFilePrefixes;

  public HiddenFilterSelectionPolicy(Config config) {
    if (config.hasPath(HIDDEN_FILTER_HIDDEN_FILE_PREFIX_KEY)) {
      this.hiddenFilePrefixes = ConfigUtils.getStringList(config, HIDDEN_FILTER_HIDDEN_FILE_PREFIX_KEY);
    } else {
      this.hiddenFilePrefixes = Arrays.asList(DEFAULT_HIDDEN_FILE_PREFIXES);
    }
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return FileSystemDatasetVersion.class;
  }

  private boolean isPathHidden(Path path) {
    while (path != null) {
      String name = path.getName();
      for (String prefix : this.hiddenFilePrefixes) {
        if (name.startsWith(prefix)) {
          return true;
        }
      }
      path = path.getParent();
    }
    return false;
  }

  private Predicate<FileSystemDatasetVersion> getSelectionPredicate() {
    return new Predicate<FileSystemDatasetVersion>() {
      @Override
      public boolean apply(FileSystemDatasetVersion version) {
        Set<Path> paths = version.getPaths();
        for (Path path : paths) {
          Path p = path.getPathWithoutSchemeAndAuthority(path);
          if (isPathHidden(p)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  @Override
  public Collection<FileSystemDatasetVersion> listSelectedVersions(List<FileSystemDatasetVersion> allVersions) {
    return Lists.newArrayList(Collections2.filter(allVersions, getSelectionPredicate()));
  }
}
