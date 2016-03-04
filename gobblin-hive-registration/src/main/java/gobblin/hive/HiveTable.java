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

package gobblin.hive;

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.annotation.Alpha;
import lombok.Getter;
import lombok.Setter;


/**
 * A class that represents a Hive table.
 *
 * <p>
 *   This class is used in {@link gobblin.hive.spec.HiveSpec} instead of Hive's
 *   {@link org.apache.hadoop.hive.metastore.api.Table} class to minimize the dependency on the hive-metastore API
 *   (since it is unstable and may go through backward incompatible changes). {@link HiveTable} and Hive's
 *   {@link org.apache.hadoop.hive.metastore.api.Table} can be converted to each other using
 *   {@link gobblin.hive.metastore.HiveMetaStoreUtils}.
 * </p>
 *
 * @author ziliu
 */
@Getter
@Alpha
public class HiveTable extends HiveRegistrationUnit {

  @Setter
  private List<Column> partitionKeys;

  private Optional<String> owner;
  private Optional<String> tableType;
  private Optional<Long> retention;

  private HiveTable(Builder builder) {
    super(builder);
    this.partitionKeys = ImmutableList.<Column> copyOf(builder.partitionKeys);
  }

  @Override
  protected void populateTablePartitionFields() {
    super.populateTablePartitionFields();
    this.owner = Optional.fromNullable(this.props.getProp(HiveConstants.OWNER));
    this.tableType = Optional.fromNullable(this.props.getProp(HiveConstants.TABLE_TYPE));
    this.retention = this.props.contains(HiveConstants.RETENTION)
        ? Optional.of(this.props.getPropAsLong(HiveConstants.RETENTION)) : Optional.<Long> absent();
  }

  public void setOwner(String owner) {
    this.owner = Optional.of(owner);
  }

  public void setTableType(String tableType) {
    this.tableType = Optional.of(tableType);
  }

  public void setRetention(long retention) {
    this.retention = Optional.of(retention);
  }

  @Override
  protected void updateTablePartitionFields(String key, Object value) {
    super.updateTablePartitionFields(key, value);
    switch (key) {
      case HiveConstants.OWNER:
        this.owner = Optional.of((String) value);
        break;
      case HiveConstants.TABLE_TYPE:
        this.tableType = Optional.of((String) value);
        break;
      case HiveConstants.RETENTION:
        this.retention = Optional.of((long) value);
        break;
    }
  }

  public static class Builder extends HiveRegistrationUnit.Builder<Builder> {
    private List<Column> partitionKeys = Lists.newArrayList();

    public Builder withPartitionKeys(List<Column> partitionKeys) {
      this.partitionKeys = partitionKeys;
      return this;
    }

    @Override
    public HiveTable build() {
      return new HiveTable(this);
    }
  }

}
