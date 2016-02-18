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

package gobblin.hive.spec;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.annotation.Alpha;
import lombok.Getter;


/**
 * A base implementation of {@link HiveSpec}.
 *
 * @author ziliu
 */
@Getter
@Alpha
public class SimpleHiveSpec implements HiveSpec {

  protected final Path path;
  protected final Table table;
  protected final Optional<Partition> partition;

  protected SimpleHiveSpec(Builder<?> builder) {
    this.path = builder.path;
    this.table = builder.table;
    this.partition = builder.partition;
  }

  public static class Builder<T extends Builder<?>> {
    private final Path path;

    private Table table;
    private Optional<Partition> partition = Optional.absent();

    public Builder(Path path) {
      this.path = path;
    }

    @SuppressWarnings("unchecked")
    public T withTable(Table table) {
      this.table = table;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPartition(Optional<Partition> partition) {
      this.partition = partition;
      return (T) this;
    }

    public SimpleHiveSpec build() {
      Preconditions.checkNotNull(this.path);
      Preconditions.checkNotNull(this.table);

      return new SimpleHiveSpec(this);
    }
  }

}
