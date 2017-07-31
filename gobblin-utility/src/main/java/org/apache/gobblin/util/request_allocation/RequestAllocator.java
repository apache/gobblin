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

import java.util.Comparator;
import java.util.Iterator;


/**
 * This interface is intended to solve the problem of selecting a subset of expensive requests based on priority when
 * there are limited resources to perform such requests.
 *
 * <p>
 * We assume there are a number of {@link Requestor}s each one sending a finite stream of {@link Request}s.
 * Each such request requires a certain amount of resources, and there is a finite pool of resources available.
 * Additionally, some requests have higher priority than others. Our objective is to select a set of requests to satisfy
 * such that their total resource usage is within the bounds of the finite resource pool, and such that, as much as
 * possible, a request will not be selected if there was a request with a higher priority that was not selected.
 * </p>
 *
 * <p>
 * We model the problem as follows:
 * <ol>
 * <li> A request is an implementation of {@link Request}. </li>
 * <li> A {@link Requestor} is a stream ({@link Iterator}) of {@link Request}s. We use a stream as opposed to a set or
 * list because the {@link Requestor} is encouraged to lazily materialize requests only as needed by the request allocator. </li>
 * <li> A {@link ResourcePool} is a vector of doubles representing the available resources along a sequence of dimensions
 * (e.g. bytes, files to copy). </li>
 * <li> A {@link ResourceRequirement} is a vector of doubles representing the resources need by a particular request.
 * We assume that resource requirements are combined exclusively through vector addition. </li>
 * <li> A {@link ResourceEstimator} is a class that, given a {@link Request}, computes its {@link ResourceRequirement}. </li>
 * <li> A prioritizer is a {@link Comparator} which, given two {@link Request}s, determines which is higher priority
 * (smaller is higher priority, following the {@link java.util.PriorityQueue} model).</li>
 * </ol>
 * </p>
 *
 * @param <T>
 */
public interface RequestAllocator<T extends Request<T>> {

  interface Factory {
    /**
     * Create a {@link RequestAllocator} with the input prioritizer and {@link ResourceEstimator}.
     */
    <T extends Request<T>> RequestAllocator<T> createRequestAllocator(RequestAllocatorConfig<T> configuration);
  }

  /**
   * Compute the subset of accepted {@link Request}s from the input {@link Requestor}s which fit withing
   * the {@link ResourcePool}.
   */
  AllocatedRequestsIterator<T> allocateRequests(Iterator<? extends Requestor<T>> requestors, ResourcePool resourcePool);
}
