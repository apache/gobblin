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

package org.apache.gobblin.hive;

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * A class that represents a Hive table.
 *
 * <p>
 *   This class is used in {@link org.apache.gobblin.hive.spec.HiveSpec} instead of Hive's
 *   {@link org.apache.hadoop.hive.metastore.api.Table} class to minimize the dependency on the hive-metastore API
 *   (since it is unstable and may go through backward incompatible changes). {@link HiveTable} and Hive's
 *   {@link org.apache.hadoop.hive.metastore.api.Table} can be converted to each other using
 *   {@link org.apache.gobblin.hive.metastore.HiveMetaStoreUtils}.
 * </p>
 *
 * @author Ziyang Liu
 */
@Getter
@Alpha
@ToString
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

  @SuppressWarnings("serial")
  @Override
  protected void populateTablePartitionFields(State state) {
    super.populateTablePartitionFields(state);
    this.owner = populateField(state, HiveConstants.OWNER, new TypeToken<String>() {});
    this.tableType = populateField(state, HiveConstants.TABLE_TYPE, new TypeToken<String>() {});
    this.retention = populateField(state, HiveConstants.RETENTION, new TypeToken<Long>() {});
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
  protected void updateTablePartitionFields(State state, String key, Object value) {
    super.updateTablePartitionFields(state, key, value);
    boolean isExistingField = true;
    switch (key) {
      case HiveConstants.OWNER:
        this.owner = Optional.of((String) value);
        break;
      case HiveConstants.TABLE_TYPE:
        this.tableType = Optional.of((String) value);
        break;
      case HiveConstants.RETENTION:
        this.retention = Optional.of((Long) value);
        break;
      default:
        isExistingField = false;
    }
    if (isExistingField) {
      state.removeProp(key);
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
