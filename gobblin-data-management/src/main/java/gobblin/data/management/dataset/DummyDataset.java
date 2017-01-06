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

package gobblin.data.management.dataset;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.retention.dataset.CleanableDataset;


/**
 * Dummy {@link Dataset} that does nothing.
 */
@RequiredArgsConstructor
public class DummyDataset implements CopyableDataset, CleanableDataset {

  private final Path datasetRoot;

  @Override public void clean() throws IOException {
    // Do nothing
  }

  @Override public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs,
      CopyConfiguration configuration)
      throws IOException {
    return ImmutableList.of();
  }

  @Override public Path datasetRoot() {
    return this.datasetRoot;
  }

  @Override public String datasetURN() {
    return datasetRoot().toString();
  }
}
