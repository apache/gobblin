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

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of a basic FiniteStateMachine that allows keeping track of the state its state and gating certain
 * logic on whether a transition is valid or not.
 *
 * This class is useful in situations where logic is complex, possibly multi-threaded, and can take multiple paths. Certain
 * pieces of logic (for example running a job, publishing a dataset, etc) can only happen if other actions ended correctly,
 * and the FSM is a way of simplifying the encoding and verification of those conditions. It is understood that state
 * transitions may not be instantaneous, and that other state transitions should not start until the current one has
 * been resolved.
 *
 * All public methods of this class will wait until the FSM is in a non-transitioning state. If multiple transitions are
 * queued at the same time, the order in which they are executed is essentially random.
 *
 * The states supported by FSM can be enums or instances of any base type. The legality of a transition is determined
 * by equality, i.e. if a transition A -> B is legal, the current state is A' and the desired end state is B', the transition
 * will be legal if A.equals(A') && B.equals(B'). This allows for storing additional information into the current state
 * as long as it does not affect the equality check (i.e. fields that are not compared in the equals check can store
 * state metadata, etc.).
 *
 * Suggested Usage:
 * FiniteStateMachine<MySymbols> fsm = new FiniteStateMachine.Builder().addTransition(START_SYMBOL, END_SYMBOL).build(initialSymbol);
 *
 * try (Transition transition = fsm.startTransition(MY_END_STATE)) {
 *   try {
 *     // my logic
 *   } catch (MyException exc) {
 *     transition.changeEndState(MY_ERROR);
 *   }
 * } catch (UnallowedTransitionException exc) {
 *   // Cannot execute logic because it's an illegal transition!
 * } catch (ReentrantStableStateWait exc) {
 * 	 // Somewhere in the logic an instruction tried to do an operation with the fsm that would likely cause a deadlock
 * } catch (AbandonedTransitionException exc) {
 *   // Another thread initiated a transition and became inactive ending the transition
 * } catch (InterruptedException exc) {
 *   // Could not start transition because thread got interrupted while waiting for a non-transitioning state
 * }
 *
 * @param <T>
 */
@Slf4j
public class FiniteStateMachine<T> {

	/**
	 * Used to build a {@link FiniteStateMachine} instance.
	 */
	public static class Builder<T> {
		private final SetMultimap<T, T> allowedTransitions;
		private final Set<T> universalEnds;

		public Builder() {
			this.allowedTransitions = HashMultimap.create();
			this.universalEnds = new HashSet<>();
		}

		/**
		 * Add a legal transition to the {@link FiniteStateMachine}.
		 */
		public Builder<T> addTransition(T startState, T endState) {
			this.allowedTransitions.put(startState, endState);
			return this;
		}

		/**
		 * Specify that a state is a valid end state for a transition starting from any state. Useful for example for
		 * error states.
		 */
		public Builder<T> addUniversalEnd(T state) {
			this.universalEnds.add(state);
			return this;
		}

		/**
		 * Build a {@link FiniteStateMachine} starting at the given initial state.
		 */
		public FiniteStateMachine<T> build(T initialState) {
			return new FiniteStateMachine<>(this.allowedTransitions, this.universalEnds, initialState);
		}
	}

	private final SetMultimap<T, T> allowedTransitions;
	private final Set<T> universalEnds;

	private final ReentrantLock lock;
	private final Condition condition;
	private final T initialState;

	private volatile T currentState;
	private volatile Transition currentTransition;

	private FiniteStateMachine(SetMultimap<T, T> allowedTransitions, Set<T> universalEnds, T initialState) {
		this.allowedTransitions = allowedTransitions;
		this.universalEnds = universalEnds;

		this.lock = new ReentrantLock();
		this.condition = this.lock.newCondition();
		this.initialState = initialState;
		this.currentState = initialState;
	}

	/**
	 * Start a transition to the end state specified. The returned {@link Transition} object is a closeable that will finalize
	 * the transition when it is closed. While the transition is open, no other transition can start.
	 *
	 * It is recommended to call this method only within a try-with-resource block to ensure the transition is closed.
	 *
	 * @throws UnallowedTransitionException If the transition is not allowed.
	 * @throws InterruptedException if the thread got interrupted while waiting for a non-transitioning state.
	 */
	public Transition startTransition(T endState) throws UnallowedTransitionException, InterruptedException {
		try {
			this.lock.lock();
			while (isTransitioning()) {
				this.condition.await(1, TimeUnit.SECONDS);
			}
			if (!isAllowedTransition(this.currentState, endState)) {
				throw new UnallowedTransitionException(this.currentState, endState);
			}
			Transition transition = new Transition(endState);
			this.currentTransition = transition;
			return transition;
		} finally {
			this.lock.unlock();
		}
	}

	/**
	 * Transition immediately to the given end state. This is essentially {@link #startTransition(Object)} immediately
	 * followed by {@link Transition#close()}.
	 *
	 * @throws UnallowedTransitionException if the transition is not allowed.
	 * @throws InterruptedException if the thread got interrupted while waiting for a non-transitioning state.
	 */
	public void transitionImmediately(T endState) throws UnallowedTransitionException, InterruptedException {
		Transition transition = startTransition(endState);
		transition.close();
	}

	/**
	 * Transition immediately to the given end state if the transition is allowed.
	 *
	 * @return true if the transition happened.
	 * @throws InterruptedException if the thread got interrupted while waiting for a non-transitioning state.
	 */
	public boolean transitionIfAllowed(T endState) throws InterruptedException {
		try {
			transitionImmediately(endState);
		} catch (UnallowedTransitionException exc) {
			return false;
		}
		return true;
	}

	/**
	 * Get the current state. This method will wait until the FSM is in a non-transitioning state (although a transition
	 * may start immediately after).
	 * @throws InterruptedException if the thread got interrupted while waiting for a non-transitioning state.
	 */
	public T getCurrentState() throws InterruptedException {
		try {
			this.lock.lock();
			while (isTransitioning()) {
				this.condition.await(1, TimeUnit.SECONDS);
			}
			return this.currentState;
		} finally {
			this.lock.unlock();
		}
	}

	@VisibleForTesting
	T getCurrentStateEvenIfTransitioning() {
		try {
			this.lock.lock();
			return this.currentState;
		} finally {
			this.lock.unlock();
		}
	}

	/**
	 * @return A clone of this FSM starting at the initial state of the FSM.
	 */
	public FiniteStateMachine<T> cloneAtInitialState() {
		return new FiniteStateMachine<>(this.allowedTransitions, this.universalEnds, this.initialState);
	}

	/**
	 * @return A clone of this FSM starting at the current state of the FSM.
	 */
	public FiniteStateMachine<T> cloneAtCurrentState() throws InterruptedException {
		try {
			this.lock.lock();
			while (isTransitioning()) {
				this.condition.await(1, TimeUnit.SECONDS);
			}
			return new FiniteStateMachine<>(this.allowedTransitions, this.universalEnds, this.currentState);
		} finally {
			this.lock.unlock();
		}
	}

	private boolean isTransitioning() {
		if (!this.lock.isHeldByCurrentThread()) {
			throw new IllegalStateException("Must hold the lock to check for transitioning state. This is an error in code.");
		}
		if (this.currentTransition != null && Thread.currentThread().equals(this.currentTransition.ownerThread)) {
			throw new ReentrantStableStateWait(
					"Tried to check for non-transitioning state from a thread that had already initiated a transition, "
							+ "this may indicate a deadlock. To change end state use Transition.changeEndState() instead.");
		}
		if (this.currentTransition != null && !this.currentTransition.ownerThread.isAlive()) {
			throw new AbandonedTransitionException(this.currentTransition.ownerThread);
		}
		return this.currentTransition != null;
	}

	private boolean isAllowedTransition(T startState, T endState) {
		if (this.universalEnds.contains(endState)) {
			return true;
		}
		Set<T> endStates = this.allowedTransitions.get(startState);
		return endStates != null && endStates.contains(endState);
	}

	/**
	 * A handle used for controlling the transition of the {@link FiniteStateMachine}. Note if this handle is lost the
	 * {@link FiniteStateMachine} will likely go into an invalid state.
	 */
	public class Transition implements Closeable {
		private final Thread ownerThread;
		private volatile T endState;
		private volatile boolean closed;

		private Transition(T endState) {
			this.ownerThread = Thread.currentThread();
			this.endState = endState;
			this.closed = false;
		}

		/**
		 * Get the state at the beginning of this transition.
		 */
		public T getStartState() {
			if (this.closed) {
				throw new IllegalStateException("Transition already closed.");
			}
			return FiniteStateMachine.this.currentState;
		}

		/**
		 * Change the end state of the transition. The new end state must be a legal transition for the state when the
		 * {@link Transition} was created.
		 *
		 * @throws UnallowedTransitionException if the new end state is not an allowed transition.
		 */
		public synchronized void changeEndState(T endState) throws UnallowedTransitionException {
			if (this.closed) {
				throw new IllegalStateException("Transition already closed.");
			}
			if (!isAllowedTransition(FiniteStateMachine.this.currentState, endState)) {
				throw new UnallowedTransitionException(FiniteStateMachine.this.currentState, endState);
			}
			this.endState = endState;
		}

		/**
		 * Close the current transition moving the {@link FiniteStateMachine} to the end state and releasing all locks.
		 */
		@Override
		public synchronized void close() {
			if (this.closed) {
				return;
			}
			this.closed = true;
			try {
				FiniteStateMachine.this.lock.lock();
				FiniteStateMachine.this.currentState = this.endState;
				FiniteStateMachine.this.currentTransition = null;
				FiniteStateMachine.this.condition.signalAll();
			} finally {
				FiniteStateMachine.this.lock.unlock();
			}
		}
	}

	/**
	 * If a transition is not allowed to happen.
	 */
	@Getter
	public static class UnallowedTransitionException extends Exception {
		private final Object startState;
		private final Object endState;

		public UnallowedTransitionException(Object startState, Object endState) {
			super(String.format("Unallowed transition: %s -> %s", startState, endState));
			this.startState = startState;
			this.endState = endState;
		}
	}

	/**
	 * Thrown when a thread that has started a transition is waiting for a non-transitioning state, which is a deadlock situation.
	 */
	public static class ReentrantStableStateWait extends RuntimeException {
		public ReentrantStableStateWait(String message) {
			super(message);
		}
	}

	/**
	 * Thrown when a transition was initiated by a thread that no longer exists, likely implying that the transition can
	 * never be closed.
	 */
	public static class AbandonedTransitionException extends RuntimeException {
		private final Thread startingThread;

		public AbandonedTransitionException(Thread startingThread) {
			super(String.format("Thread %s initiated a transition but became inactive before closing it.", startingThread));
			this.startingThread = startingThread;
		}
	}
}
