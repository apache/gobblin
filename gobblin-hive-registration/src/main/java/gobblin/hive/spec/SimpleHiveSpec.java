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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import gobblin.annotation.Alpha;
import gobblin.hive.HivePartition;
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
  protected final String dbName;
  protected final String tableName;
  protected final Optional<HivePartition> partition;
  protected final StorageDescriptor sd;

  protected SimpleHiveSpec(Builder<?> builder) {
    this.path = builder.path;
    this.dbName = builder.dbName;
    this.tableName = builder.tableName;
    this.partition = builder.partition;
    this.sd = builder.sd;
  }

  public static class Builder<T extends Builder<?>> {
    private final Path path;

    private String dbName;
    private String tableName;
    private Optional<HivePartition> partition = Optional.absent();
    private StorageDescriptor sd;

    public Builder(Path path) {
      this.path = path;
    }

    @SuppressWarnings("unchecked")
    public T withDbName(String dbName) {
      this.dbName = dbName;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withTableName(String tableName) {
      this.tableName = tableName;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPartition(HivePartition partition) {
      this.partition = Optional.of(partition);
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPartition(Optional<HivePartition> partition) {
      this.partition = partition;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withStorageDescriptor(StorageDescriptor sd) {
      this.sd = sd;
      return (T) this;
    }

    public SimpleHiveSpec build() {
      Preconditions.checkState(this.path != null);
      Preconditions.checkState(!Strings.isNullOrEmpty(this.dbName));
      Preconditions.checkState(!Strings.isNullOrEmpty(this.tableName));
      Preconditions.checkState(this.sd != null);

      return new SimpleHiveSpec(this);
    }
  }
}
