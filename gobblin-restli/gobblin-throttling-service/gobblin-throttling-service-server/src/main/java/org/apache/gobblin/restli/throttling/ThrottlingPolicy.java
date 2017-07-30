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

package gobblin.restli.throttling;

import java.util.Map;


/**
 * A throttling policy.
 */
public interface ThrottlingPolicy {
  /**
   * Comput the {@link PermitAllocation} for the given {@link PermitRequest}.
   */
  PermitAllocation computePermitAllocation(PermitRequest request);

  /**
   * @return The parameters this policy is using. Used for description of the policy.
   */
  Map<String, String> getParameters();

  /**
   * @return  A human readable description of the policy.
   */
  String getDescription();
}
