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

import lombok.Getter;

/*
The instance calling this method acquired the lease for the event in question. The class contains the `eventTimestamp`
associated with the lease as well as the time the lease was obtained by me or `myLeaseAcquisitionTimestamp`.
 */
public class LeaseObtainedStatus extends LeaseAttemptStatus {
  @Getter
  private final long eventTimestamp;

  @Getter
  private final long myLeaseAcquisitionTimestamp;

  protected LeaseObtainedStatus(long eventTimestamp, long myLeaseAcquisitionTimestamp) {
    super();
    this.eventTimestamp = eventTimestamp;
    this.myLeaseAcquisitionTimestamp = myLeaseAcquisitionTimestamp;
  }
}
