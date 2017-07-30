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

package gobblin.hive;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.annotation.Alpha;
import lombok.Getter;


/**
 * A class that represents a Hive partition.
 *
 * <p>
 *   This class is used in {@link gobblin.hive.spec.HiveSpec} instead of Hive's
 *   {@link org.apache.hadoop.hive.metastore.api.Partition} class to minimize the dependency on the hive-metastore API
 *   (since it is unstable and may go through backward incompatible changes). {@link HivePartition} and Hive's
 *   {@link org.apache.hadoop.hive.metastore.api.Partition} can be converted to each other using
 *   {@link gobblin.hive.metastore.HiveMetaStoreUtils}.
 * </p>
 *
 * @author Ziyang Liu
 */
@Getter
@Alpha
public class HivePartition extends HiveRegistrationUnit {

  private final List<String> values;

  private HivePartition(Builder builder) {
    super(builder);
    this.values = ImmutableList.<String> copyOf(builder.values);
  }

  @Override
  public String toString() {
    return super.toString() + " Values: " + this.values.toString();
  }

  public static class Builder extends HiveRegistrationUnit.Builder<Builder> {

    private List<String> values = Lists.newArrayList();

    public Builder withPartitionValues(List<String> values) {
      this.values = values;
      return this;
    }

    @Override
    public HivePartition build() {
      return new HivePartition(this);
    }

  }
}
