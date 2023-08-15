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
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import org.apache.gobblin.dataset.IterableDatasetFinder;


@Slf4j
public class ManifestBasedDatasetFinder implements IterableDatasetFinder<ManifestBasedDataset> {

  public static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".manifestBased";
  public static final String MANIFEST_LOCATION = CONFIG_PREFIX + ".manifest.location";
  public static final String MANIFEST_READ_FS_URI = CONFIG_PREFIX + ".read.fs.uri";
  private final FileSystem srcFs;
  private final FileSystem manifestReadFs;
  private final List<Path> manifestLocations;
  private final  Properties properties;
  public ManifestBasedDatasetFinder(final FileSystem srcFs, Properties properties) {
    Preconditions.checkArgument(properties.containsKey(MANIFEST_LOCATION), "Manifest location key required in config. Please set " + MANIFEST_LOCATION);
    this.srcFs = srcFs;
    final Optional<String> optManifestReadFsUriStr = Optional.ofNullable(properties.getProperty(MANIFEST_READ_FS_URI));
    try {
      // config may specify a `FileSystem` other than `srcFs` solely for reading manifests; fallback is to use `srcFs`
      this.manifestReadFs = optManifestReadFsUriStr.isPresent()
          ? FileSystem.get(URI.create(optManifestReadFsUriStr.get()), new Configuration())
          : srcFs;
      log.info("using file system to read manifest files: '{}'", this.manifestReadFs.getUri());
    } catch (final IOException | IllegalArgumentException e) {
      throw new RuntimeException("unable to create manifest-loading FS at URI '" + optManifestReadFsUriStr + "'", e);
    }
    manifestLocations = new ArrayList<>();
    this.properties = properties;
    Splitter.on(',').trimResults().split(properties.getProperty(MANIFEST_LOCATION)).forEach(s -> manifestLocations.add(new Path(s)));
  }
  @Override
  public List<ManifestBasedDataset> findDatasets() throws IOException {
    return manifestLocations.stream().map(p -> new ManifestBasedDataset(srcFs, manifestReadFs, p, properties)).collect(Collectors.toList());
  }

  @Override
  public Path commonDatasetRoot() {
    return new Path("/");
  }

  @Override
  public Iterator<ManifestBasedDataset> getDatasetsIterator() throws IOException {
    return manifestLocations.stream().map(p -> new ManifestBasedDataset(srcFs, manifestReadFs, p, properties)).iterator();
  }
}
