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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;


/**
 * Represents a pool of available resources for a {@link RequestAllocator}. The resources pool is essentially a vector
 * of doubles where each dimension represents a resource. A set of resource requests exceeds the availability of the
 * pool if the vector sum of those requests is larger than the vector of resources in the pool along any dimension.
 */
@Getter(value = AccessLevel.PROTECTED)
public class ResourcePool {

  public static final double DEFAULT_DIMENSION_TOLERANCE = 1.2;

  private final ImmutableMap<String, Integer> dimensionIndex;
  private final double[] softBound;
  private final double[] hardBound;
  private final double[] defaultResourceUse;

  /**
   * @param maxResources Maximum resource availability along each dimension. Each entry in this map is a dimension. Note
   *                     this is considered a soft bound (e.g. max resources may be exceeded by a tolerance).
   * @param tolerances The hard limit on resources availability along each dimension is set to maxResource * tolerance.
   *                   The default tolerance is {@link #DEFAULT_DIMENSION_TOLERANCE}. It is recommended to always have a
   *                   tolerance >1, as some {@link RequestAllocator}s will do unnecessary work if the soft and hard
   *                   bounds are too close to each other.
   * @param defaultRequirements Specifies the default usage of the resources along each dimension when creating a
   *                            {@link ResourceRequirement}. Default is 0.
   */
  @Builder
  protected ResourcePool(@Singular Map<String, Double> maxResources, @Singular Map<String, Double> tolerances,
      @Singular Map<String, Double> defaultRequirements) {

    ImmutableMap.Builder<String, Integer> indexBuilder = ImmutableMap.builder();
    this.softBound = new double[maxResources.size()];

    int currentIndex = 0;
    for (Map.Entry<String, Double> resource : maxResources.entrySet()) {
      indexBuilder.put(resource.getKey(), currentIndex);
      this.softBound[currentIndex] = resource.getValue();
      currentIndex++;
    }
    this.dimensionIndex = indexBuilder.build();

    this.hardBound = this.softBound.clone();
    for (int i = 0; i < this.hardBound.length; i++) {
      this.hardBound[i] *= DEFAULT_DIMENSION_TOLERANCE;
    }
    this.defaultResourceUse = new double[this.softBound.length];

    for (Map.Entry<String, Integer> idxEntry : this.dimensionIndex.entrySet()) {
      if (tolerances.containsKey(idxEntry.getKey())) {
        this.hardBound[idxEntry.getValue()] =
            this.softBound[idxEntry.getValue()] * Math.max(1.0, tolerances.get(idxEntry.getKey()));
      }
      if (defaultRequirements.containsKey(idxEntry.getKey())) {
        this.defaultResourceUse[idxEntry.getValue()] = defaultRequirements.get(idxEntry.getKey());
      }
    }
  }

  private ResourcePool(double[] softBound, double[] hardBound, double[] defaultResourceUse, ImmutableMap<String, Integer> dimensionIndex) {
    this.softBound = softBound;
    this.hardBound = hardBound;
    this.defaultResourceUse = defaultResourceUse;
    this.dimensionIndex = dimensionIndex;
  }

  protected ResourcePool(ResourcePool other) {
    this.softBound = other.getSoftBound();
    this.hardBound = other.getHardBound();
    this.defaultResourceUse = other.getDefaultResourceUse();
    this.dimensionIndex = other.getDimensionIndex();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(ResourcePool.class.getSimpleName()).append(": {");
    builder.append("softBound").append(": ").append(vectorToString(this.softBound));
    builder.append(", ");
    builder.append("hardBound").append(": ").append(vectorToString(this.hardBound));
    builder.append("}");
    return builder.toString();
  }

  /**
   * Stringify a {@link ResourceRequirement} with the appropriate dimension labels.
   */
  public String stringifyRequirement(ResourceRequirement requirement) {
    return vectorToString(requirement.getResourceVector());
  }

  private String vectorToString(double[] vector) {
    List<String> tokens = Lists.newArrayListWithCapacity(this.dimensionIndex.size());
    for (Map.Entry<String, Integer> dimension : dimensionIndex.entrySet()) {
      tokens.add(dimension.getKey() + ": " + vector[dimension.getValue()]);
    }
    return Arrays.toString(tokens.toArray());
  }

  /**
   * @return true if input {@link ResourceRequirement} exceeds the soft bound long any dimension. If the parameter
   *        orEqual is true, then matching along any dimension will also return true.
   */
  public boolean exceedsSoftBound(ResourceRequirement requirement, boolean orEqual) {
    return VectorAlgebra.exceedsVector(this.softBound, requirement.getResourceVector(), orEqual);
  }

  /**
   * @return true if input {@link ResourceRequirement} exceeds the hard bound long any dimension. If the parameter
   *        orEqual is true, then matching along any dimension will also return true.
   */
  public boolean exceedsHardBound(ResourceRequirement requirement, boolean orEqual) {
    return VectorAlgebra.exceedsVector(this.hardBound, requirement.getResourceVector(), orEqual);
  }

  /**
   * Use to create a {@link ResourceRequirement} compatible with this {@link ResourcePool}.
   */
  public ResourceRequirement.Builder getResourceRequirementBuilder() {
    return new ResourceRequirement.Builder(this);
  }

  /**
   * @return a new {@link ResourcePool} which is a copy of this {@link ResourcePool} except its resource vector has been
   * reduced by the input {@link ResourceRequirement}.
   */
  protected ResourcePool contractPool(ResourceRequirement requirement) {
    return new ResourcePool(VectorAlgebra.addVector(this.softBound, requirement.getResourceVector(), -1., null),
        VectorAlgebra.addVector(this.hardBound, requirement.getResourceVector(), -1., null),
        this.defaultResourceUse, this.dimensionIndex);
  }

  /**
   * Get the dimensionality of the embedded resource vector.
   */
  int getNumDimensions() {
    return this.dimensionIndex.size();
  }

  double[] getDefaultResourceUse(double[] reuse) {
    if (reuse != null && this.defaultResourceUse.length == reuse.length) {
      System.arraycopy(this.defaultResourceUse, 0, reuse, 0, this.defaultResourceUse.length);
      return reuse;
    }
    return this.defaultResourceUse.clone();
  }
}
