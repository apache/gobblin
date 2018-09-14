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

package org.apache.gobblin.dataset;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import lombok.Getter;


/**
 * A {@link Descriptor} to identifies a partition of a dataset
 */
public class PartitionDescriptor extends Descriptor {

  /** Type token for ser/de partition descriptor list */
  private static final Type DESCRIPTOR_LIST_TYPE = new TypeToken<ArrayList<PartitionDescriptor>>(){}.getType();

  @Getter
  private final DatasetDescriptor dataset;

  public PartitionDescriptor(String name, DatasetDescriptor dataset) {
    super(name);
    this.dataset = dataset;
  }

  @Override
  public PartitionDescriptor copy() {
    return new PartitionDescriptor(getName(), dataset);
  }

  public PartitionDescriptor copyWithNewDataset(DatasetDescriptor dataset) {
    return new PartitionDescriptor(getName(), dataset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionDescriptor that = (PartitionDescriptor) o;
    return dataset.equals(that.dataset) && getName().equals(that.getName());
  }

  @Override
  public int hashCode() {
    int result = dataset.hashCode();
    result = 31 * result + getName().hashCode();
    return result;
  }

  /**
   * Serialize a list of partition descriptors as json string
   */
  public static String toPartitionJsonList(List<PartitionDescriptor> descriptors) {
    return Descriptor.GSON.toJson(descriptors, DESCRIPTOR_LIST_TYPE);
  }

  /**
   * Deserialize the string, resulted from {@link #toPartitionJsonList(List)}, to a list of partition descriptors
   */
  public static List<PartitionDescriptor> fromPartitionJsonList(String jsonList) {
    return Descriptor.GSON.fromJson(jsonList, DESCRIPTOR_LIST_TYPE);
  }
}
