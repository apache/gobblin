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
package org.apache.gobblin.compaction.verify;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.gobblin.dataset.Dataset;


/**
 * An interface which represents a generic verifier for compaction
 */
public interface CompactionVerifier<D extends Dataset> {

   @Getter
   @AllArgsConstructor
   class Result {
      boolean isSuccessful;
      String failureReason;
   }

   String COMPACTION_VERIFIER_PREFIX = "compaction-verifier-";
   String COMPACTION_VERIFICATION_TIMEOUT_MINUTES = "compaction.verification.timeoutMinutes";
   String COMPACTION_VERIFICATION_ITERATION_COUNT_LIMIT = "compaction.verification.iteration.countLimit";
   String COMPACTION_VERIFICATION_THREADS = "compaction.verification.threads";
   String COMPACTION_VERIFICATION_FAIL_REASON = "compaction.verification.fail.reason";

   Result verify (D dataset);

   boolean isRetriable ();
   String getName ();
}