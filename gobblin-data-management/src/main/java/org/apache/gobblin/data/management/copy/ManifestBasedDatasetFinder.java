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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ManifestBasedDatasetFinder implements IterableDatasetFinder<ManifestBasedDataset> {

  public static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".manifestBased";
  public static final String MANIFEST_LOCATION = CONFIG_PREFIX + ".manifest.location";
  private final FileSystem fs;
  private final List<Path> manifestLocations;
  private final  Properties properties;
  public ManifestBasedDatasetFinder(final FileSystem fs, Properties properties) {
    Preconditions.checkArgument(properties.containsKey(MANIFEST_LOCATION), "Manifest location key required in config. Please set " + MANIFEST_LOCATION);
    this.fs = fs;
    manifestLocations = new ArrayList<>();
    this.properties = properties;
    Splitter.on(',').trimResults().split(properties.getProperty(MANIFEST_LOCATION)).forEach(s -> manifestLocations.add(new Path(s)));
  }
  @Override
  public List<ManifestBasedDataset> findDatasets() throws IOException {
    return manifestLocations.stream().map(p -> new ManifestBasedDataset(fs, p, properties)).collect(Collectors.toList());
  }

  @Override
  public Path commonDatasetRoot() {
    return new Path("/");
  }

  @Override
  public Iterator<ManifestBasedDataset> getDatasetsIterator() throws IOException {
    return manifestLocations.stream().map(p -> new ManifestBasedDataset(fs, p, properties)).iterator();
  }
}
