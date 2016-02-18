/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.partition;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.dataset.Dataset;

/**
 * A named subset of {@link File}s in a {@link Dataset}. (Useful for partitions, versions, etc.)
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class FileSet<T extends File> {

  public static class Builder<T extends File> {

    private final String name;
    private final List<T> files;
    private final Dataset dataset;

    public Builder(String name, Dataset dataset) {
      if (name == null) {
        throw new RuntimeException("Name cannot be null.");
      }
      this.name = name;
      this.files = Lists.newArrayList();
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
      return new FileSet<>(this.name, ImmutableList.copyOf(this.files), this.dataset);
    }
  }

  @NonNull private final String name;
  private final ImmutableList<T> files;
  private final Dataset dataset;

}
