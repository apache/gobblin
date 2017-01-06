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

package gobblin.util.request_allocation;

import java.io.Serializable;
import java.util.Comparator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
@Getter
public class RequestAllocatorConfig<T extends Request<T>> {
  private final Comparator<T> prioritizer;
  private final ResourceEstimator<T> resourceEstimator;
  private final int allowedThreads;
  private Config limitedScopeConfig;

  public static <T extends Request<T>> Builder<T> builder(ResourceEstimator<T> resourceEstimator) {
    return new Builder<>(resourceEstimator);
  }

  public static class Builder<T extends Request<T>> {
    private Comparator<T> prioritizer = new AllEqualPrioritizer<>();
    private final ResourceEstimator<T> resourceEstimator;
    private int allowedThreads = 1;
    private Config limitedScopeConfig;

    public Builder(ResourceEstimator<T> resourceEstimator) {
      this.resourceEstimator = resourceEstimator;
    }

    public Builder<T> allowParallelization() {
      return allowParallelization(20);
    }

    public Builder<T> allowParallelization(int maxThreads) {
      this.allowedThreads = maxThreads;
      return this;
    }

    public Builder<T> withLimitedScopeConfig(Config config) {
      this.limitedScopeConfig = config;
      return this;
    }

    public Builder<T> withPrioritizer(Comparator<T> prioritizer) {
      this.prioritizer = prioritizer;
      return this;
    }

    public RequestAllocatorConfig<T> build() {
      if (this.limitedScopeConfig == null) {
        this.limitedScopeConfig = ConfigFactory.empty();
      }
      return new RequestAllocatorConfig<>(this.prioritizer, this.resourceEstimator, this.allowedThreads, this.limitedScopeConfig);
    }
  }

  public static class AllEqualPrioritizer<T> implements Comparator<T>, Serializable {
    @Override
    public int compare(T o1, T o2) {
      return 0;
    }
  }

}
