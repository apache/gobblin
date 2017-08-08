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

import java.util.Iterator;
import java.util.Random;

import com.google.common.base.Function;

import lombok.Getter;


/**
 * A basic implementation of {@link AllocatedRequestsIterator}.
 * @param <T>
 */
public class AllocatedRequestsIteratorBase<T extends Request<T>> implements AllocatedRequestsIterator<T> {

  private final Iterator<RequestWithResourceRequirement<T>> underlying;
  private final double[] currentRequirement;

  public AllocatedRequestsIteratorBase(Iterator<RequestWithResourceRequirement<T>> underlying, ResourcePool resourcePool) {
    this.underlying = underlying;
    this.currentRequirement = new double[resourcePool.getNumDimensions()];
  }

  @Override
  public ResourceRequirement totalResourcesUsed() {
    return new ResourceRequirement(this.currentRequirement);
  }

  @Override
  public boolean hasNext() {
    return this.underlying.hasNext();
  }

  @Override
  public T next() {
    RequestWithResourceRequirement<T> nextElement = this.underlying.next();
    VectorAlgebra.addVector(this.currentRequirement, nextElement.getResourceRequirement().getResourceVector(), 1.0,
        this.currentRequirement);
    return nextElement.getT();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Stores and element and its {@link ResourceRequirement}.
   */
  @Getter
  public static class RequestWithResourceRequirement<T> {
    public static final Random RANDOM = new Random();

    private final T t;
    private final ResourceRequirement resourceRequirement;
    private final long id;

    RequestWithResourceRequirement(T t, ResourceRequirement resourceRequirement) {
      this.t = t;
      this.resourceRequirement = resourceRequirement;
      this.id = RANDOM.nextLong();
    }
  }

  /**
   * A {@link Function} used to extract the actual {@link Request} from a {@link RequestWithResourceRequirement}.
   */
  public static class TExtractor<T> implements Function<RequestWithResourceRequirement<T>, T> {
    @Override
    public T apply(RequestWithResourceRequirement<T> ttAndRequirement) {
      return ttAndRequirement.getT();
    }
  }
}
