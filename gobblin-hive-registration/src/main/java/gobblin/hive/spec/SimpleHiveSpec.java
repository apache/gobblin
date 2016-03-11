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
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import gobblin.annotation.Alpha;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveTable;
import gobblin.hive.HiveRegister;
import gobblin.hive.spec.activity.Activity;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.Collection;


/**
 * A base implementation of {@link HiveSpec}.
 *
 * @author ziliu
 */
@Getter
@Alpha
@Builder(builderClassName = "Builder")
public class SimpleHiveSpec implements HiveSpec, HiveSpecWithPreActivities, HiveSpecWithPostActivities,
    HiveSpecWithPredicates {

  protected final Path path;
  protected final HiveTable table;
  protected final Optional<HivePartition> partition;
  @Singular @Getter
  protected final Collection<Activity> preActivities;
  @Singular @Getter
  protected final Collection<Activity> postActivities;
  @Singular @Getter
  protected final Collection<Predicate<HiveRegister>> predicates;

  protected SimpleHiveSpec(Builder<?> builder) {
    this.path = builder.path;
    this.table = builder.table;
    this.partition = builder.partition != null ? builder.partition : Optional.<HivePartition>absent();
    this.preActivities = builder.preActivities;
    this.postActivities = builder.postActivities;
    this.predicates = builder.predicates;
  }

  private static Builder<Builder<?>> builder() {
    return new Builder<>();
  }

  private static Builder<Builder<?>> builder(Path path) {
    return new Builder<>(path);
  }

  public static class Builder<T extends Builder<?>> {
    private Path path;

    private Builder() {
      this.path = null;
    }

    public Builder(Path path) {
      this.path = path;
      this.preActivities = Lists.newArrayList();
      this.postActivities = Lists.newArrayList();
      this.predicates = Lists.newArrayList();
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
