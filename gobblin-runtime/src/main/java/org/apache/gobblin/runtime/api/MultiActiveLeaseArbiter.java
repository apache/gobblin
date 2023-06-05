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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This interface defines a generic approach to a non-blocking, multiple active thread or host system, in which one or
 * more active instances compete over ownership of a particular flow's event. The type of flow event in question does
 * not impact the algorithm other than to uniquely identify the flow event. Each instance uses the interface to initiate
 * an attempt at ownership over the flow event and receives a response indicating the status of the attempt.
 *
 * At a high level the lease arbiter works as follows:
 *  1. Multiple instances receive knowledge of a flow action event to act upon
 *  2. Each instance attempts to acquire rights or `a lease` to be the sole instance acting on the event by calling the
 *      tryAcquireLease method below and receives the resulting status. The status indicates whether this instance has
 *        a) acquired the lease -> then this instance will attempt to complete the lease
 *        b) another has acquired the lease -> then another will attempt to complete the lease
 *        c) flow event no longer needs to be acted upon -> terminal state
 *  3. If another has acquired the lease, then the instance will check back in at the time of lease expiry to see if it
 *    needs to attempt the lease again [status (b) above].
 *  4. Once the instance which acquired the lease completes its work on the flow event, it calls completeLeaseUse to
 *    indicate to all other instances that the flow event no longer needs to be acted upon [status (c) above]
 */
public interface MultiActiveLeaseArbiter {
  static final Logger LOG = LoggerFactory.getLogger(MultiActiveLeaseArbiter.class);

  /**
   * This method attempts to insert an entry into store for a particular flow action event if one does not already
   * exist in the store for the flow action or has expired. Regardless of the outcome it also reads the lease
   * acquisition timestamp of the entry for that flow action event (it could have pre-existed in the table or been newly
   *  added by the previous write). Based on the transaction results, it will return @LeaseAttemptStatus to determine
   *  the next action.
   * @param flowAction uniquely identifies the flow
   * @param eventTimeMillis is the time this flow action should occur
   * @return LeaseAttemptStatus
   * @throws IOException
   */
  LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis) throws IOException;

  /**
   * This method is used to indicate the owner of the lease has successfully completed required actions while holding
   * the lease of the flow action event. It marks the lease as "no longer leasing", if the eventTimeMillis and
   * leaseAcquisitionTimeMillis values have not changed since this owner acquired the lease (indicating the lease did
   * not expire).
   * @return true if successfully updated, indicating no further actions need to be taken regarding this event.
   */
  boolean completeLeaseUse(DagActionStore.DagAction flowAction, long eventTimeMillis, long leaseAcquisitionTimeMillis)
      throws IOException;
}
