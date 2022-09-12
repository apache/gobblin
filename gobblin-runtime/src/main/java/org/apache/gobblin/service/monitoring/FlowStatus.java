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

package org.apache.gobblin.service.monitoring;

import java.util.Iterator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.ExecutionStatus;


/**
 * Represents status of a flow.
 */
@Alpha
@AllArgsConstructor
@Getter
@ToString
public class FlowStatus {
  private final String flowName;
  private final String flowGroup;
  private final long flowExecutionId;
  @ToString.Exclude // (to avoid side-effecting exhaustion of `Iterator`)
  private final Iterator<JobStatus> jobStatusIterator;
  private final ExecutionStatus flowExecutionStatus;
}
