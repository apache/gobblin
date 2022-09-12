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
package org.apache.gobblin.runtime.messaging.data;

import java.util.List;
import lombok.Builder;
import lombok.Value;

/**
 * Message for the task runner to request the AM to split a workunit into multiple workunits
 * because a subset of topic partitions are lagging in the specified workunit.
 */
@Value
@Builder
public class SplitWorkUnitMessage implements DynamicWorkUnitMessage {
  /**
   * Workunit ID of the work unit that should be split into multiple smaller workunits
   */
  String workUnitId;

  /**
   * Topic partitions that have been lagging in the workunit
   */
  List<String> laggingTopicPartitions;
}
