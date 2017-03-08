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
package gobblin.compliance;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import lombok.AllArgsConstructor;

import gobblin.configuration.State;
import gobblin.dataset.Dataset;


/**
 * A version finder class to find {@link HivePartitionVersion}s.
 *
 * @author adsharma
 */
@AllArgsConstructor
public abstract class HivePartitionVersionFinder implements gobblin.data.management.version.finder.VersionFinder<HivePartitionVersion> {
  protected final FileSystem fs;
  protected final State state;
  protected List<String> patterns;

  @Override
  public Class<HivePartitionVersion> versionClass() {
    return HivePartitionVersion.class;
  }

  @Override
  public abstract Collection<HivePartitionVersion> findDatasetVersions(Dataset dataset)
      throws IOException;
}
