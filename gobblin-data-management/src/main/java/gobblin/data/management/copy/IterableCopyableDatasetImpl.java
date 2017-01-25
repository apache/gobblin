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

package gobblin.data.management.copy;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import gobblin.data.management.partition.FileSet;
import gobblin.dataset.Dataset;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import org.apache.hadoop.fs.FileSystem;


/**
 * Wraps a {@link CopyableDataset} to produce an {@link IterableCopyableDataset}.
 */
@AllArgsConstructor
public class IterableCopyableDatasetImpl implements IterableCopyableDataset {

  private final CopyableDataset dataset;

  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    return partitionCopyableFiles(this.dataset, this.dataset.getCopyableFiles(targetFs, configuration));
  }

  @Override
  public String datasetURN() {
    return this.dataset.datasetURN();
  }

  private static Iterator<FileSet<CopyEntity>> partitionCopyableFiles(Dataset dataset,
      Collection<? extends CopyEntity> files) {
    Map<String, FileSet.Builder<CopyEntity>> partitionBuildersMaps = Maps.newHashMap();
    for (CopyEntity file : files) {
      if (!partitionBuildersMaps.containsKey(file.getFileSet())) {
        partitionBuildersMaps.put(file.getFileSet(), new FileSet.Builder<>(file.getFileSet(), dataset));
      }
      partitionBuildersMaps.get(file.getFileSet()).add(file);
    }
    return Iterators.transform(partitionBuildersMaps.values().iterator(),
        new Function<FileSet.Builder<CopyEntity>, FileSet<CopyEntity>>() {
          @Nullable
          @Override
          public FileSet<CopyEntity> apply(@Nonnull FileSet.Builder<CopyEntity> input) {
            return input.build();
          }
        });
  }
}
