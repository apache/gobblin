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
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * } catch (FailedTransitionCallbackException exc) {
 *   // A callback in the transition start or end states has failed.
 *   exc.getTransition().changeEndState(MY_ERROR).closeWithoutCallbacks(); // example handling
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
    private T errorState;

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
     * Specify the error state to which this machine can transition if nothing else is possible. Note the error state
     * is always an allowed end state.
     */
		public Builder<T> errorState(T state) {
		  this.errorState = state;
		  return this;
    }

		/**
		 * Build a {@link FiniteStateMachine} starting at the given initial state.
		 */
		public FiniteStateMachine<T> build(T initialState) {
			return new FiniteStateMachine<>(this.allowedTransitions, this.universalEnds, this.errorState, initialState);
		}
	}

	private final SetMultimap<T, T> allowedTransitions;
	private final Set<T> universalEnds;
	private final T errorState;

	private final ReentrantReadWriteLock lock;
	private final Condition condition;
	private final T initialState;

	private volatile T currentState;
	private volatile Transition currentTransition;

	protected FiniteStateMachine(SetMultimap<T, T> allowedTransitions, Set<T> universalEnds, T errorState, T initialState) {
		this.allowedTransitions = allowedTransitions;
		this.universalEnds = universalEnds;
		this.errorState = errorState;

		this.lock = new ReentrantReadWriteLock();
		this.condition = this.lock.writeLock().newCondition();
		this.initialState = initialState;
		this.currentState = initialState;

		if (this.currentState instanceof StateWithCallbacks) {
		  ((StateWithCallbacks) this.currentState).onEnterState(null);
    }
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
			this.lock.writeLock().lock();
			while (isTransitioning()) {
				this.condition.await();
			}
			if (!isAllowedTransition(this.currentState, endState)) {
				throw new UnallowedTransitionException(this.currentState, endState);
			}
			Transition transition = new Transition(endState);
			this.currentTransition = transition;
			return transition;
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	/**
	 * Transition immediately to the given end state. This is essentially {@link #startTransition(Object)} immediately
	 * followed by {@link Transition#close()}.
	 *
	 * @throws UnallowedTransitionException if the transition is not allowed.
	 * @throws InterruptedException if the thread got interrupted while waiting for a non-transitioning state.
	 */
	public void transitionImmediately(T endState) throws UnallowedTransitionException, InterruptedException, FailedTransitionCallbackException {
		Transition transition = startTransition(endState);
		transition.close();
	}

	/**
	 * Transition immediately to the given end state if the transition is allowed.
	 *
	 * @return true if the transition happened.
	 * @throws InterruptedException if the thread got interrupted while waiting for a non-transitioning state.
	 */
	public boolean transitionIfAllowed(T endState) throws InterruptedException, FailedTransitionCallbackException {
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
		  // Need to get lock to make sure we're not in transitioning state.
			this.lock.readLock().lock();

			waitForNonTransitioningReadLock();

			return this.currentState;
		} finally {
			this.lock.readLock().unlock();
		}
	}

	@VisibleForTesting
	T getCurrentStateEvenIfTransitioning() {
		return this.currentState;
	}

	/**
	 * @return A clone of this FSM starting at the initial state of the FSM.
	 */
	public FiniteStateMachine<T> cloneAtInitialState() {
		return new FiniteStateMachine<>(this.allowedTransitions, this.universalEnds, this.errorState, this.initialState);
	}

	/**
	 * @return A clone of this FSM starting at the current state of the FSM.
	 */
	public FiniteStateMachine<T> cloneAtCurrentState() throws InterruptedException {
		try {
			this.lock.readLock().lock();

			waitForNonTransitioningReadLock();

			return new FiniteStateMachine<>(this.allowedTransitions, this.universalEnds, this.errorState, this.currentState);
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Waits for a read lock in a non-transitioning state. The caller MUST hold the read lock before calling this method.
	 * @throws InterruptedException
	 */
	private void waitForNonTransitioningReadLock() throws InterruptedException {
		if (isTransitioning()) {
			this.lock.readLock().unlock();
			// To use the condition, need to upgrade to a write lock
			this.lock.writeLock().lock();
			try {
				while (isTransitioning()) {
					this.condition.await();
				}
				// After non-transitioning state, downgrade again to read-lock
				this.lock.readLock().lock();
			} finally {
				this.lock.writeLock().unlock();
			}
		}
	}

	private boolean isTransitioning() {
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

	protected boolean isAllowedTransition(T startState, T endState) {
	  if (endState.equals(this.errorState)) {
	    return true;
    }
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
     * Change the end state of the transition to the FSM error state.
     */
		public synchronized void switchEndStateToErrorState() {
		  this.endState = FiniteStateMachine.this.errorState;
    }

		/**
		 * Close the current transition moving the {@link FiniteStateMachine} to the end state and releasing all locks.
     *
     * @throws FailedTransitionCallbackException when start or end state callbacks fail. Note if this exception is thrown
     * the transition is not complete and the error must be handled to complete it.
		 */
		@Override
		public void close() throws FailedTransitionCallbackException {
			doClose(true);
		}

    /**
     * Close the current transition moving the {@link FiniteStateMachine} to the end state and releasing all locks without
     * calling any callbacks. This method should only be called after a {@link #close()} has failed and the failure
     * cannot be handled.
     */
		public void closeWithoutCallbacks() {
		  try {
        doClose(false);
      } catch (FailedTransitionCallbackException exc) {
		    throw new IllegalStateException(String.format("Close without callbacks threw a %s. This is an error in code.",
            FailedTransitionCallbackException.class), exc);
      }
    }

		private synchronized void doClose(boolean withCallbacks) throws FailedTransitionCallbackException {
      if (this.closed) {
        return;
      }

      try {
        FiniteStateMachine.this.lock.writeLock().lock();

        try {
          if (withCallbacks && getStartState() instanceof StateWithCallbacks) {
            ((StateWithCallbacks<T>) getStartState()).onLeaveState(this.endState);
          }
        } catch (Throwable t) {
          throw new FailedTransitionCallbackException(this, FailedCallback.START_STATE, t);
        }

        try {
          if (withCallbacks && this.endState instanceof StateWithCallbacks) {
            ((StateWithCallbacks) this.endState).onEnterState(getStartState());
          }
        } catch (Throwable t) {
          throw new FailedTransitionCallbackException(this, FailedCallback.END_STATE, t);
        }

        this.closed = true;

        FiniteStateMachine.this.currentState = this.endState;
        FiniteStateMachine.this.currentTransition = null;
        FiniteStateMachine.this.condition.signalAll();
      } finally {
        FiniteStateMachine.this.lock.writeLock().unlock();
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

	public enum FailedCallback {
	  START_STATE, END_STATE
  }

  /**
   * Thrown when the callbacks when closing a transition fail.
   */
  @Getter
	public static class FailedTransitionCallbackException extends IOException {
	  private final FiniteStateMachine.Transition transition;
	  private final FailedCallback failedCallback;
	  private final Throwable originalException;

    public FailedTransitionCallbackException(FiniteStateMachine<?>.Transition transition, FailedCallback failedCallback,
        Throwable originalException) {
      super("Failed callbacks when ending transition.", originalException);
      this.transition = transition;
      this.failedCallback = failedCallback;
      this.originalException = originalException;
    }
  }
}
