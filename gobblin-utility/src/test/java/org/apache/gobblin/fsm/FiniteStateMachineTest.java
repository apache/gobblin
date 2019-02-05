package org.apache.gobblin.fsm;

import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import lombok.Data;
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

	@Test(timeOut = 5000)
	public void multiThreadTest() throws Exception {
		FiniteStateMachine<MyStates> fsm = refFsm.cloneAtInitialState();

		Assert.assertEquals(fsm.getCurrentState(), MyStates.PENDING);

		Transitioner t1 = new Transitioner(fsm, MyStates.RUNNING);
		Transitioner t2 = new Transitioner(fsm, MyStates.ERROR);

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
	private class Transitioner implements Runnable {
		private final FiniteStateMachine<MyStates> fsm;
		private final MyStates endState;

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
		STARTING, TRANSITIONING, COMPLETED, INTERRUPTED, UNALLOWED, TIMEOUT
	}
}
