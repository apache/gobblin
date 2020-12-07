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

import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class FiniteStateMachineTest {

	public enum MyStates {
		PENDING, RUNNING, SUCCESS, ERROR
	}

	private final FiniteStateMachine<MyStates> refFsm = new FiniteStateMachine.Builder<MyStates>()
			.addTransition(MyStates.PENDING, MyStates.RUNNING)
			.addTransition(MyStates.RUNNING, MyStates.SUCCESS)
			.addTransition(MyStates.PENDING, MyStates.ERROR)
			.addTransition(MyStates.RUNNING, MyStates.ERROR).build(MyStates.PENDING);

	@Test
	public void singleThreadImmediateTransitionsTest() throws Exception {
		FiniteStateMachine<MyStates> fsm = refFsm.cloneAtInitialState();

		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);
		fsm.transitionImmediately(MyStates.RUNNING);
		Assert.assertEquals(fsm.getCurrentState(), MyStates.RUNNING);
		fsm.transitionImmediately(MyStates.SUCCESS);
		Assert.assertEquals(fsm.getCurrentState(), MyStates.SUCCESS);

		fsm = fsm.cloneAtInitialState();
		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);
		fsm.transitionImmediately(MyStates.ERROR);
		Assert.assertEquals(fsm.getCurrentState(), MyStates.ERROR);

		fsm = fsm.cloneAtCurrentState();
		Assert.assertEquals(fsm.getCurrentState(), MyStates.ERROR);
	}

	@Test
	public void illegalTransitionsTest() throws Exception {
		FiniteStateMachine<MyStates> fsm = refFsm.cloneAtInitialState();

		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);
		try {
			fsm.transitionImmediately(MyStates.PENDING);
			Assert.fail();
		} catch (FiniteStateMachine.UnallowedTransitionException exc) {
			// expected
		}
		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);

		try {
			fsm.transitionImmediately(MyStates.SUCCESS);
			Assert.fail();
		} catch (FiniteStateMachine.UnallowedTransitionException exc) {
			// expected
		}
		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);

		fsm.transitionImmediately(MyStates.RUNNING);
		Assert.assertEquals(fsm.getCurrentState(), MyStates.RUNNING);
	}

	@Test
	public void slowTransitionsTest() throws Exception {
		FiniteStateMachine<MyStates> fsm = refFsm.cloneAtInitialState();

		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);
		try (FiniteStateMachine.Transition transition = fsm.startTransition(MyStates.RUNNING)) {
			try {
				fsm.getCurrentState();
				Assert.fail();
			} catch (FiniteStateMachine.ReentrantStableStateWait exc) {
				// Expected because the same thread that is transitioning tries to read the current state
			}
			try {
				fsm.transitionImmediately(MyStates.RUNNING);
				Assert.fail();
			} catch (FiniteStateMachine.ReentrantStableStateWait exc) {
				// Expected because the same thread that is transitioning tries to start another transition
			}
		}
		Assert.assertEquals(fsm.getCurrentState(), MyStates.RUNNING);

		try (FiniteStateMachine<MyStates>.Transition transition = fsm.startTransition(MyStates.SUCCESS)) {
			transition.changeEndState(MyStates.ERROR);
		}
		Assert.assertEquals(fsm.getCurrentState(), MyStates.ERROR);
	}

	@Test
	public void callbackTest() throws Exception {
	  NamedStateWithCallback stateA = new NamedStateWithCallback("a");
    NamedStateWithCallback stateB = new NamedStateWithCallback("b");
    NamedStateWithCallback stateC = new NamedStateWithCallback("c", null, s -> {
      throw new RuntimeException("leave");
    });
    NamedStateWithCallback stateD = new NamedStateWithCallback("d");

    FiniteStateMachine<NamedStateWithCallback> fsm = new FiniteStateMachine.Builder<NamedStateWithCallback>()
        .addTransition(new NamedStateWithCallback("a"), new NamedStateWithCallback("b"))
        .addTransition(new NamedStateWithCallback("b"), new NamedStateWithCallback("c"))
        .addTransition(new NamedStateWithCallback("c"), new NamedStateWithCallback("d"))
        .addUniversalEnd(new NamedStateWithCallback("ERROR"))
        .build(stateA);

    fsm.transitionImmediately(stateB);

    Assert.assertEquals(fsm.getCurrentState(), stateB);
    Assert.assertEquals(stateA.lastTransition, "leave:a->b");
    stateA.lastTransition = "";
    Assert.assertEquals(stateB.lastTransition, "enter:a->b");
    stateB.lastTransition = "";

    try {
      // State that will error on enter
      fsm.transitionImmediately(new NamedStateWithCallback("c", s -> {
        throw new RuntimeException("enter");
      }, s -> {
        throw new RuntimeException("leave");
      }));
      Assert.fail("Expected excpetion");
    } catch (FiniteStateMachine.FailedTransitionCallbackException exc) {
      Assert.assertEquals(exc.getFailedCallback(), FiniteStateMachine.FailedCallback.END_STATE);
      Assert.assertEquals(exc.getOriginalException().getMessage(), "enter");
      // switch state to one that will only error on leave
      exc.getTransition().changeEndState(stateC);
      exc.getTransition().close();
    }

    Assert.assertEquals(fsm.getCurrentState(), stateC);
    Assert.assertEquals(stateB.lastTransition, "leave:b->c");
    stateB.lastTransition = "";
    Assert.assertEquals(stateC.lastTransition, "enter:b->c");
    stateC.lastTransition = "";

    try {
      fsm.transitionImmediately(stateD);
      Assert.fail("Expected exception");
    } catch (FiniteStateMachine.FailedTransitionCallbackException exc) {
      Assert.assertEquals(exc.getFailedCallback(), FiniteStateMachine.FailedCallback.START_STATE);
      Assert.assertEquals(exc.getOriginalException().getMessage(), "leave");
      // switch state to one that will only error on leave
      exc.getTransition().changeEndState(new NamedStateWithCallback("ERROR"));
      exc.getTransition().closeWithoutCallbacks();
    }

    Assert.assertEquals(fsm.getCurrentState(), new NamedStateWithCallback("ERROR"));
    Assert.assertEquals(stateD.lastTransition, "");
  }

	@Test(timeOut = 5000)
	public void multiThreadTest() throws Exception {
		FiniteStateMachine<MyStates> fsm = refFsm.cloneAtInitialState();

		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);

		Transitioner<MyStates> t1 = new Transitioner<>(fsm, MyStates.RUNNING);
		Transitioner<MyStates> t2 = new Transitioner<>(fsm, MyStates.ERROR);

		Thread t1Thread = new Thread(null, t1, "t1");
		t1Thread.start();
		t1.awaitState(Sets.newHashSet(TransitionState.TRANSITIONING));
		Assert.assertEquals(t1.transitionResult, TransitionState.TRANSITIONING);
		Assert.assertEquals(fsm.getCurrentStateEvenIfTransitioning(), MyStates.PENDING);

		Thread t2Thread = new Thread(null, t2, "t2");
		t2Thread.start();
		Assert.assertEquals(t1.transitionResult, TransitionState.TRANSITIONING);
		Assert.assertEquals(t2.transitionResult, TransitionState.STARTING);
		Assert.assertEquals(fsm.getCurrentStateEvenIfTransitioning(), MyStates.PENDING);

		t1Thread.interrupt();
		t1.awaitState(Sets.newHashSet(TransitionState.COMPLETED));
		t2.awaitState(Sets.newHashSet(TransitionState.TRANSITIONING));
		Assert.assertEquals(t1.transitionResult, TransitionState.COMPLETED);
		Assert.assertEquals(t2.transitionResult, TransitionState.TRANSITIONING);
		Assert.assertEquals(fsm.getCurrentStateEvenIfTransitioning(), MyStates.RUNNING);

		t2Thread.interrupt();
		t2.awaitState(Sets.newHashSet(TransitionState.COMPLETED));
		Assert.assertEquals(t1.transitionResult, TransitionState.COMPLETED);
		Assert.assertEquals(t2.transitionResult, TransitionState.COMPLETED);
		Assert.assertEquals(fsm.getCurrentStateEvenIfTransitioning(), MyStates.ERROR);
	}

	@Test(timeOut = 5000)
	public void deadThreadTest() throws Exception {
		FiniteStateMachine<MyStates> fsm = refFsm.cloneAtInitialState();

		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);

		Thread t = new Thread(() -> {
			try {
				FiniteStateMachine.Transition transition = fsm.startTransition(MyStates.RUNNING);
			} catch (FiniteStateMachine.UnallowedTransitionException | InterruptedException exc) {
				// do nothing
			}
			// since we don't close the transition, it should become orphaned
		});
		t.start();

		while (t.isAlive()) {
			Thread.sleep(50);
		}

		try {
			fsm.transitionImmediately(MyStates.RUNNING);
			Assert.fail();
		} catch (FiniteStateMachine.AbandonedTransitionException exc) {
			// Expected
		}
	}

	@Data
	private class Transitioner<T> implements Runnable {
		private final FiniteStateMachine<T> fsm;
		private final T endState;

		private final Lock lock = new ReentrantLock();
		private final Condition condition = lock.newCondition();

		private volatile boolean running = false;
		private volatile TransitionState transitionResult = TransitionState.STARTING;

		@Override
		public void run() {
			try(FiniteStateMachine.Transition transition = this.fsm.startTransition(this.endState)) {
				goToState(TransitionState.TRANSITIONING);
				try {
					Thread.sleep(2000);
					this.transitionResult = TransitionState.TIMEOUT;
					return;
				} catch (InterruptedException ie) {
					// This is the signal to end the state transition, so do nothing
				}
			} catch (InterruptedException exc) {
				goToState(TransitionState.INTERRUPTED);
				return;
			} catch (FiniteStateMachine.UnallowedTransitionException exc) {
				goToState(TransitionState.UNALLOWED);
				return;
			} catch (FiniteStateMachine.FailedTransitionCallbackException exc) {
				goToState(TransitionState.CALLBACK_ERROR);
				return;
			}
			goToState(TransitionState.COMPLETED);
		}

		public void awaitState(Set<TransitionState> states) throws InterruptedException {
			try {
				this.lock.lock();
				while (!states.contains(this.transitionResult)) {
					this.condition.await();
				}
			} finally {
				this.lock.unlock();
			}
		}

		public void goToState(TransitionState state) {
			try {
				this.lock.lock();
				this.transitionResult = state;
				this.condition.signalAll();
			} finally {
				this.lock.unlock();
			}
		}
	}

	enum TransitionState {
		STARTING, TRANSITIONING, COMPLETED, INTERRUPTED, UNALLOWED, TIMEOUT, CALLBACK_ERROR
	}

	@RequiredArgsConstructor
  @EqualsAndHashCode(of = "name")
	public static class NamedStateWithCallback implements StateWithCallbacks<NamedStateWithCallback> {
	  @Getter
	  private final String name;
	  private final Function<NamedStateWithCallback, Void> enterCallback;
	  private final Function<NamedStateWithCallback, Void> leaveCallback;

	  String lastTransition = "";

    public NamedStateWithCallback(String name) {
      this(name, null, null);
    }

    private void setLastTransition(String callback, NamedStateWithCallback start, NamedStateWithCallback end) {
      this.lastTransition = String.format("%s:%s->%s", callback, start == null ? "null" : start.name, end.name);
    }

    @Override
    public void onEnterState(@Nullable NamedStateWithCallback previousState) {
      if (this.enterCallback == null) {
        setLastTransition("enter", previousState, this);
      } else {
        this.enterCallback.apply(previousState);
      }
    }

    @Override
    public void onLeaveState(NamedStateWithCallback nextState) {
      if (this.leaveCallback == null) {
        setLastTransition("leave", this, nextState);
      } else {
        this.leaveCallback.apply(nextState);
      }
    }
  }
}
