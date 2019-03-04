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

package org.apache.gobblin.fsm;

import javax.annotation.Nullable;


/**
 * A state for a {@link FiniteStateMachine} which supports callbacks when entering and leaving the state.
 * @param <T> supertype of states in the FSM.
 */
public interface StateWithCallbacks<T> {

	/**
	 * Called when an FSM reaches this state.
	 * @param previousState the previous state of the machine.
	 */
	default void onEnterState(@Nullable T previousState) {
		// do nothing
	}

	/**
	 * Called when an FSM leaves this state.
	 * @param nextState the next state of the machine.
	 */
	default void onLeaveState(T nextState) {
		// do nothing
	}

}
