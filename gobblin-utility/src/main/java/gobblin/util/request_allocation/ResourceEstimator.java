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

package gobblin.util.request_allocation;

import com.typesafe.config.Config;


/**
 * Computes the {@link ResourceRequirement} for {@link Request}s. See {@link RequestAllocator}.
 * @param <T>
 */
public interface ResourceEstimator<T> {

  interface Factory<T> {
    ResourceEstimator<T> create(Config config);
  }

  /**
   * @return The {@link ResourceRequirement} for input {@link Request}.
   */
  ResourceRequirement estimateRequirement(T t, ResourcePool resourcePool);
}
