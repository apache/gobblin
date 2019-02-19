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
