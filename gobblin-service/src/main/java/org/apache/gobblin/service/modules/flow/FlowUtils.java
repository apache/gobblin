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
package org.apache.gobblin.service.modules.flow;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
public class FlowUtils {
  /**
   * A FlowSpec contains a FlowExecutionId if it is a runOnce flow.
   * Refer {@link FlowConfigResourceLocalHandler#createFlowSpecForConfig} for details.
   * @param spec flow spec
   * @return flow execution id
   */
  public static long getOrCreateFlowExecutionId(FlowSpec spec) {
    long flowExecutionId;
    if (spec.getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      flowExecutionId = spec.getConfig().getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    } else {
      flowExecutionId = System.currentTimeMillis();
    }
    return flowExecutionId;
  }
}