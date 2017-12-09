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

package org.apache.gobblin.hive.spec;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.spec.activity.Activity;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.Collection;


/**
 * A base implementation of {@link HiveSpec}.
 */
@Getter
@Alpha
@Builder(builderClassName = "Builder")
public class SimpleHiveSpec
    implements HiveSpec, HiveSpecWithPreActivities, HiveSpecWithPostActivities, HiveSpecWithPredicates {

  protected final Path path;
  protected final HiveTable table;
  protected final Optional<HivePartition> partition;
  @Singular
  @Getter
  protected final Collection<Activity> preActivities;
  @Singular
  @Getter
  protected final Collection<Activity> postActivities;
  @Singular
  @Getter
  protected final Collection<Predicate<HiveRegister>> predicates;

  protected SimpleHiveSpec(Builder<?> builder) {
    this.path = builder.path;
    this.table = builder.table;
    this.partition = builder.partition != null ? builder.partition : Optional.<HivePartition> absent();
    this.preActivities = builder.preActivities;
    this.postActivities = builder.postActivities;
    this.predicates = builder.predicates;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).omitNullValues().add("path", this.path.toString())
        .add("db", this.table.getDbName()).add("table", this.table.getTableName())
        .add("partition", this.partition.orNull()).toString();
  }

  public static class Builder<T extends Builder<?>> {
    @Getter
    private Path path;

    @Getter
    private HiveTable table;

    @Getter
    private Optional<HivePartition> partition;

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
