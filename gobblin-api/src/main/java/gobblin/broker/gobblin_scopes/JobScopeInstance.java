/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.broker.gobblin_scopes;

import lombok.Getter;


/**
 * A {@link gobblin.broker.iface.ScopeInstance} for a Gobblin job.
 */
@Getter
public class JobScopeInstance extends GobblinScopeInstance {

  private final String jobName;
  private final String jobId;

  public JobScopeInstance(String jobName, String jobId) {
    super(GobblinScopeTypes.JOB, jobName + ", id=" + jobId);
    this.jobName = jobName;
    this.jobId = jobId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    JobScopeInstance that = (JobScopeInstance) o;

    if (!jobName.equals(that.jobName)) {
      return false;
    }
    return jobId.equals(that.jobId);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + jobName.hashCode();
    result = 31 * result + jobId.hashCode();
    return result;
  }
}
