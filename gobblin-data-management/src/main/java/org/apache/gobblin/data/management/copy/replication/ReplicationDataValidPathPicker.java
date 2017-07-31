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

package org.apache.gobblin.data.management.copy.replication;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Ordering;
import com.typesafe.config.Config;

import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.dataset.FileSystemDataset;

/**
 * Used to pick the valid Paths for data replication based on {@link Config}
 * @author mitu
 *
 */
public class ReplicationDataValidPathPicker {

  public static final String POLICY_CLASS = "selection.policy.class";

  public static final String FINDER_CLASS = "version.finder.class";

  @SuppressWarnings("unchecked")
  public static Collection<Path> getValidPaths(HadoopFsEndPoint hadoopFsEndPoint) throws IOException{
    Config selectionConfig = hadoopFsEndPoint.getSelectionConfig();

    FileSystemDataset tmpDataset = new HadoopFsEndPointDataset(hadoopFsEndPoint);
    FileSystem theFs = FileSystem.get(hadoopFsEndPoint.getFsURI(), new Configuration());

    /**
     * Use {@link FileSystemDatasetVersion} as
     * {@link DateTimeDatasetVersionFinder} / {@link GlobModTimeDatasetVersionFinder} use {@link TimestampedDatasetVersion}
     * {@link SingleVersionFinder} uses {@link FileStatusDatasetVersion}
     */
    VersionFinder<FileSystemDatasetVersion> finder;
    try {
      finder = (VersionFinder<FileSystemDatasetVersion>) ConstructorUtils
          .invokeConstructor(Class.forName(selectionConfig.getString(FINDER_CLASS)), theFs, selectionConfig);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }

    List<FileSystemDatasetVersion> versions =
        Ordering.natural().reverse().sortedCopy(finder.findDatasetVersions(tmpDataset));

    VersionSelectionPolicy<FileSystemDatasetVersion> selector;
    try {
      selector = (VersionSelectionPolicy<FileSystemDatasetVersion>) ConstructorUtils
          .invokeConstructor(Class.forName(selectionConfig.getString(POLICY_CLASS)), selectionConfig);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }

    Collection<FileSystemDatasetVersion> versionsSelected = selector.listSelectedVersions(versions);

    List<Path> result = new ArrayList<Path>();
    for(FileSystemDatasetVersion t: versionsSelected){
      // get the first element out
      result.add(t.getPaths().iterator().next());
    }
    return result;
  }
}
