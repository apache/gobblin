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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.gobblin.configuration.State;


/**
 * Backend-specific handling of an existing KILL action after its active DAG has been removed.
 *
 * <p>The supplied states are the only retained execution metadata source. Implementations must validate backend
 * handles, destinations and their original submission identities, without consulting archived DAGs or guessing
 * destinations from current routing. A flow-summary state is context, not an execution to stop. Implementations
 * must not rewrite terminal job status or emit normal flow/job cancellation events. Bind implementations as
 * singletons so the service manager can close the same instance used by KILL processing.</p>
 */
@FunctionalInterface
public interface ForceKillHandler extends Closeable {
  enum Result {
    /** All identified stop requests were acknowledged; this does not establish physical process termination. */
    ACKNOWLEDGED,
    /** No execution could be resolved, or at least one stop was not acknowledged. */
    NOT_ACKNOWLEDGED,
    /** An active DAG was observed; stop retained processing and let normal DAG cancellation take over. */
    ACTIVE_DAG_PRESENT
  }

  @FunctionalInterface
  interface ActiveDagCheck {
    boolean isActive() throws IOException;
  }

  /**
   * Attempt bounded, backend-specific stopping using defensive copies of the retained job states.
   *
   * @param action the exact flow execution and optional job requested for cancellation
   * @param jobStates scoped job states and, if retained, the flow-summary state; the list is unmodifiable
   * @param activeDagCheck must be checked before each backend stop; return {@link Result#ACTIVE_DAG_PRESENT} if true.
   *                       This reduces, but does not atomically exclude, concurrent resume.
   * @throws IOException when the operation cannot be completed; failures must not be reported as acknowledgements
   */
  Result forceKill(DagActionStore.DagAction action, List<State> jobStates, ActiveDagCheck activeDagCheck)
      throws IOException;

  /** Stop accepting work and release local resources without waiting indefinitely for remote calls. Must be idempotent. */
  @Override
  default void close() {
  }
}
