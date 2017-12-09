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

package org.apache.gobblin.data.management.partition;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.Singular;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.util.request_allocation.Request;
import org.apache.gobblin.util.request_allocation.Requestor;


/**
 * A named subset of {@link File}s in a {@link Dataset}. (Useful for partitions, versions, etc.).
 *
 * The actual list of files in this {@link FileSet} is, in ideal circumstances, generated lazily. As such, the method
 * {@link #getFiles()} should only be called when the actual list of files is needed.
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class FileSet<T extends CopyEntity> implements Request<FileSet<CopyEntity>> {

  /**
   * A builder for {@link StaticFileSet} provided for backwards compatibility. The output of this builder is not lazy.
   */
  public static class Builder<T extends CopyEntity> {

    private final String name;
    private final List<T> files = Lists.newArrayList();
    private final Dataset dataset;

    public Builder(String name, Dataset dataset) {
      if (name == null) {
        throw new RuntimeException("Name cannot be null.");
      }
      this.name = name;
      this.dataset = dataset;
    }

    public Builder<T> add(T t) {
      this.files.add(t);
      return this;
    }

    public Builder<T> add(Collection<T> collection) {
      this.files.addAll(collection);
      return this;
    }

    public FileSet<T> build() {
      return new StaticFileSet<>(this.name, this.dataset, this.files);
    }
  }

  @Getter
  @NonNull private final String name;
  @Getter
  private final Dataset dataset;

  private ImmutableList<T> generatedEntities;
  private long totalSize = -1;
  private int totalEntities = -1;
  @Setter
  @Getter
  private Requestor<FileSet<CopyEntity>> requestor;

  public ImmutableList<T> getFiles() {
    ensureFilesGenerated();
    return this.generatedEntities;
  }

  public long getTotalSizeInBytes() {
    ensureStatsComputed();
    return this.totalSize;
  }

  public int getTotalEntities() {
    ensureStatsComputed();
    return this.totalEntities;
  }

  private void ensureFilesGenerated() {
    if (this.generatedEntities == null) {
      try {
        this.generatedEntities = ImmutableList.copyOf(generateCopyEntities());
      } catch (Exception exc) {
        throw new RuntimeException("Failed to generate files for file set " + name, exc);
      }
      recomputeStats();
    }
  }

  private void ensureStatsComputed() {
    ensureFilesGenerated();
    if (this.totalEntities < 0 || this.totalSize < 0) {
      recomputeStats();
    }
  }

  private void recomputeStats() {
    this.totalEntities = this.generatedEntities.size();
    this.totalSize = 0;
    for (CopyEntity copyEntity : this.generatedEntities) {
      if (copyEntity instanceof CopyableFile) {
        this.totalSize += ((CopyableFile) copyEntity).getOrigin().getLen();
      }
    }
  }

  /**
   * This method is called lazily when needed and only once, it is intended to do the heavy work of generating the
   * {@link CopyEntity}s.
   * @return The {@link Collection} of {@link CopyEntity}s in this file set.
   * @throws IOException
   */
  protected abstract Collection<T> generateCopyEntities() throws IOException;

  @Override
  public String toString() {
    return this.dataset.datasetURN() + "@" + this.name;
  }
}
