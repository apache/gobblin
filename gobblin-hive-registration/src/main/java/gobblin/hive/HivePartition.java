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

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import gobblin.annotation.Alpha;
import lombok.Getter;


/**
 * Representation of a Hive partition.
 *
 * @author ziliu
 */
@Getter
@Alpha
public class HivePartition {

  private final List<FieldSchema> keys;
  private final List<String> values;

  public HivePartition(List<FieldSchema> keys, List<String> values) {
    Preconditions.checkNotNull(keys);
    Preconditions.checkNotNull(values);
    Preconditions.checkArgument(keys.size() >= 1 && keys.size() == values.size());
    this.keys = ImmutableList.copyOf(keys);
    this.values = ImmutableList.copyOf(values);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < keys.size(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(keys.get(i).getName());
      sb.append("=");
      sb.append(values.get(i));
    }
    return sb.toString();
  }

}
