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

import lombok.Getter;


/**
 * A {@link Descriptor} to identifies a partition of a dataset
 */
public class PartitionDescriptor extends Descriptor {
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

  /**
   * Create a copy of partition descriptor under a new dataset
   */
  public PartitionDescriptor copy(DatasetDescriptor dataset) {
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
}
