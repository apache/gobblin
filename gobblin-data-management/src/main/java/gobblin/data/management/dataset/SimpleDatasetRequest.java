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

package gobblin.data.management.dataset;

import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;

import gobblin.annotation.Alias;
import gobblin.dataset.Dataset;
import gobblin.util.request_allocation.Request;
import gobblin.util.request_allocation.Requestor;
import gobblin.util.request_allocation.ResourceEstimator;
import gobblin.util.request_allocation.ResourcePool;
import gobblin.util.request_allocation.ResourceRequirement;

/**
 * A simple {@link Request} which represents a {@link Dataset}
 */
@AllArgsConstructor
public class SimpleDatasetRequest implements Request<SimpleDatasetRequest> {

  public static final String SIMPLE_DATASET_COUNT_DIMENSION = "count";
  @Getter
  Dataset dataset;
  SimpleDatasetRequestor requestor;
  @Override
  public Requestor<SimpleDatasetRequest> getRequestor() {
    return requestor;
  }

  /**
   * A simple {@link ResourceEstimator} which counts {@link SimpleDatasetRequest} as the only dimension
   */
  public static class SimpleDatasetCountEstimator implements ResourceEstimator<SimpleDatasetRequest> {
    static class Factory implements ResourceEstimator.Factory<SimpleDatasetRequest> {
      @Override
      public ResourceEstimator<SimpleDatasetRequest>  create(Config config) {
        return new SimpleDatasetCountEstimator();
      }
    }

    public ResourceRequirement estimateRequirement(SimpleDatasetRequest request, ResourcePool pool) {
      return new ResourceRequirement.Builder(pool).setRequirement(SIMPLE_DATASET_COUNT_DIMENSION, 1).build();
    }
  }

  @Override
  public String toString() {
    return dataset.toString();
  }
}
