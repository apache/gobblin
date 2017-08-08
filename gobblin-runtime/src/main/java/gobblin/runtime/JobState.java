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
package gobblin.runtime;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gobblin.configuration.State;

/***
 * Shim layer for org.apache.gobblin.runtime.JobState
 */
public class JobState extends org.apache.gobblin.runtime.JobState {
  // Necessary for serialization/deserialization
  public JobState() {
  }

  public JobState(String jobName, String jobId) {
    super(jobName, jobId);
  }

  public JobState(State properties, Map<String, DatasetState> previousDatasetStates, String jobName,
      String jobId) {
    super(properties, adaptDatasetStateMap(previousDatasetStates), jobName, jobId);
  }

  private static Map<String, org.apache.gobblin.runtime.JobState.DatasetState> adaptDatasetStateMap(
      Map<String, DatasetState> previousDatasetStates) {

    return previousDatasetStates.entrySet()
    .stream()
    .collect(Collectors.toMap(Map.Entry::getKey,
        e -> new org.apache.gobblin.runtime.JobState.DatasetState(e.getValue().getJobName(), e.getValue().getId())));
  }

  /***
   * Shim layer for org.apache.gobblin.runtime.JobState.DatasetState
   */
  public static class DatasetState extends org.apache.gobblin.runtime.JobState.DatasetState {

    // For serialization/deserialization
    public DatasetState() {
      super();
    }

    public DatasetState(String jobName, String jobId) {
      super(jobName, jobId);
    }
  }
}
