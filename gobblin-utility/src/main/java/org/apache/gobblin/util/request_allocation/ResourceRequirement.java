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

package org.apache.gobblin.util.request_allocation;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Represents a requirement of resources in a {@link ResourcePool}. Essentially a vector of doubles of the same dimension
 * as that in the {@link ResourcePool}. See {@link ResourcePool}.
 */
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ResourceRequirement {

  public static class Builder {

    private final double[] requirement;
    private final ResourcePool pool;

    public Builder(ResourcePool pool) {
      this.pool = pool;
      this.requirement = pool.getDefaultResourceUse(null);
    }

    /**
     * Set all resource requirements to 0. Overrides defaults.
     */
    public Builder zero() {
      for (int i = 0; i < this.requirement.length; i++) {
        this.requirement[i] = 0;
      }
      return this;
    }

    /**
     * Specify the resource requirement along a dimension.
     */
    public Builder setRequirement(String dimension, double value) {
      if (!this.pool.getDimensionIndex().containsKey(dimension)) {
        throw new IllegalArgumentException(String.format("No dimension %s in this resource pool.", dimension));
      }
      int idx = this.pool.getDimensionIndex().get(dimension);
      this.requirement[idx] = value;
      return this;
    }

    public ResourceRequirement build() {
      return new ResourceRequirement(this.requirement);
    }
  }

  @Getter
  private final double[] resourceVector;

  public ResourceRequirement(ResourceRequirement other) {
    this.resourceVector = other.resourceVector.clone();
  }

  /**
   * Vector addition of this and other {@link ResourceRequirement}.
   */
  public void add(ResourceRequirement other) {
    VectorAlgebra.addVector(this.resourceVector, other.resourceVector, 1., this.resourceVector);
  }

  /**
   * Vector addition of this and other {@link ResourceRequirement}.
   */
  public void subtract(ResourceRequirement other) {
    VectorAlgebra.addVector(this.resourceVector, other.resourceVector, -1., this.resourceVector);
  }

  void entryWiseMax(ResourceRequirement other) {
    for (int i = 0; i < this.resourceVector.length; i ++) {
      this.resourceVector[i] = Math.max(this.resourceVector[i], other.resourceVector[i]);
    }
  }

  ResourceRequirement copyInto(ResourceRequirement reuse) {
    if (reuse == null) {
      return new ResourceRequirement(this.resourceVector.clone());
    } else {
      System.arraycopy(this.resourceVector, 0, reuse.getResourceVector(), 0, this.resourceVector.length);
      return reuse;
    }
  }

  public static ResourceRequirement add(ResourceRequirement r1, ResourceRequirement r2, ResourceRequirement reuse) {
    if (reuse == null) {
      reuse = new ResourceRequirement(r1.resourceVector.clone());
    }
    VectorAlgebra.addVector(r1.resourceVector, r2.resourceVector, 1., reuse.resourceVector);
    return reuse;
  }

}
