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
package org.apache.gobblin.service.modules.orchestration;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class AzkabanProjectFlowsStatus extends AzkabanClientStatus<AzkabanProjectFlowsStatus.Project> {
    public AzkabanProjectFlowsStatus(AzkabanProjectFlowsStatus.Project project) {
        super(project);
    }

    // Those classes represent Azkaban API response
    // For more details, see: https://azkaban.readthedocs.io/en/latest/ajaxApi.html#fetch-flows-of-a-project
    @Getter
    @AllArgsConstructor
    public static class Project {
        long projectId;
        List<Flow> flows;
    }

    @Getter
    @AllArgsConstructor
    public static class Flow {
        String flowId;
    }
}