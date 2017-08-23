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
package org.apache.gobblin.util.limiter.broker;

import org.apache.gobblin.broker.iface.SharedResourceKey;

import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * A {@link SharedResourceKey} for use with {@link SharedLimiterFactory}. The resourceLimitedPath should identify the resource
 * that will be limited, for example the uri of an external service for which calls should be throttled.
 */
@Getter
@EqualsAndHashCode
public class SharedLimiterKey implements SharedResourceKey {

  public enum GlobalLimiterPolicy {
    USE_GLOBAL,
    USE_GLOBAL_IF_CONFIGURED,
    LOCAL_ONLY
  }

  /**
   * A "/" separated path representing the resource limited. For example:
   * - /filesystem/myHDFSCluster
   * - /databases/mysql/myInstance
   */
  private final String resourceLimitedPath;
  private final GlobalLimiterPolicy globalLimiterPolicy;

  public SharedLimiterKey(String resourceLimitedPath) {
    this(resourceLimitedPath, GlobalLimiterPolicy.USE_GLOBAL_IF_CONFIGURED);
  }

  public SharedLimiterKey(String resourceLimitedPath, GlobalLimiterPolicy globalLimiterPolicy) {
    this.resourceLimitedPath = (resourceLimitedPath.startsWith("/") ? "" : "/") + resourceLimitedPath;
    this.globalLimiterPolicy = globalLimiterPolicy;
  }

  public String toString() {
    return toConfigurationKey();
  }

  @Override
  public String toConfigurationKey() {
    // remove leading "/"
    return this.resourceLimitedPath.substring(1).replace("/", ".");
  }
}
