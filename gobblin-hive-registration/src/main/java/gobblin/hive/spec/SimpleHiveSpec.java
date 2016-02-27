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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.annotation.Alpha;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveTable;
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
  protected final HiveTable table;
  protected final Optional<HivePartition> partition;

  protected SimpleHiveSpec(Builder<?> builder) {
    this.path = builder.path;
    this.table = builder.table;
    this.partition = builder.partition;
  }

  @Getter
  public static class Builder<T extends Builder<?>> {
    protected final Path path;
    protected HiveTable table;
    protected Optional<HivePartition> partition = Optional.absent();

    public Builder(Path path) {
      this.path = path;
    }

    @SuppressWarnings("unchecked")
    public T withTable(HiveTable table) {
      this.table = table;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPartition(Optional<HivePartition> partition) {
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
