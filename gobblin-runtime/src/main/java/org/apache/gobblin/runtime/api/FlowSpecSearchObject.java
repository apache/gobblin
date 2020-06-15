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

package org.apache.gobblin.runtime.api;

import java.net.URI;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.service.FlowId;


/**
 * This is a class to package all the parameters that should be used to search {@link FlowSpec} in a {@link SpecStore}
 */
@Getter
@Builder
@ToString
@AllArgsConstructor
public class FlowSpecSearchObject implements SpecSearchObject {
  private final URI flowSpecUri;
  private final String flowGroup;
  private final String flowName;
  private final String templateURI;
  private final String userToProxy;
  private final String sourceIdentifier;
  private final String destinationIdentifier;
  private final String schedule;
  private final String modifiedTimestamp;
  private final Boolean isRunImmediately;
  private final String owningGroup;
  private final String propertyFilter;

  public static FlowSpecSearchObject fromFlowId(FlowId flowId) {
    return FlowSpecSearchObject.builder().flowGroup(flowId.getFlowGroup()).flowName(flowId.getFlowName()).build();
  }
}
