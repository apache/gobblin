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
package gobblin.writer.test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import gobblin.writer.test.TestingEventBuses.Event;

import lombok.AllArgsConstructor;

/**
 * A wrapper around an EventBus created with {@link TestingEventBuses} that implements various
 * asserts on the incoming messages.
 *
 * <p><b>Important:</b> This class must be instantiated before any messages are sent on the bus or
 * it won't detect them.
 */
public class TestingEventBusAsserter implements Closeable {
  private final BlockingDeque<TestingEventBuses.Event> _events = new LinkedBlockingDeque<>();
  private final EventBus _eventBus;
  private long _defaultTimeoutValue = 1;
  private TimeUnit _defaultTimeoutUnit = TimeUnit.SECONDS;

  @AllArgsConstructor
  public static class StaticMessage implements Function<TestingEventBuses.Event, String> {
    private final String message;
    @Override public String apply(Event input) {
      return this.message;
    }
  }

  public TestingEventBusAsserter(String eventBusId) {
    _eventBus = TestingEventBuses.getEventBus(eventBusId);
    _eventBus.register(this);
  }

  @Subscribe public void processEvent(TestingEventBuses.Event e) {
    _events.offer(e);
  }

  @Override public void close() throws IOException {
    _eventBus.unregister(this);
  }

  public BlockingDeque<TestingEventBuses.Event> getEvents() {
    return _events;
  }

  public void clear() {
    _events.clear();
  }

  /** Sets timeout for all subsequent blocking asserts. */
  public TestingEventBusAsserter withTimeout(long timeout, TimeUnit unit) {
    _defaultTimeoutValue = timeout;
    _defaultTimeoutUnit = unit;
    return this;
  }

  /** Gets the next event from the queue and validates that it satisfies a given predicate. Blocking
   * assert. The event is removed from the internal queue regardless if the predicate has been
   * satisfied.
   * @param predicate the predicate to apply on the next event
   * @param assert error message generator
   * @return the event if the predicate is satisfied
   * @throws AssertionError if the predicate is not satisfied
   */
  public TestingEventBuses.Event assertNext(final Predicate<TestingEventBuses.Event> predicate,
        Function<TestingEventBuses.Event, String> messageGen
        ) throws InterruptedException, TimeoutException {
    TestingEventBuses.Event nextEvent = _events.pollFirst(_defaultTimeoutValue, _defaultTimeoutUnit);
    if (null == nextEvent) {
      throw new TimeoutException();
    }
    if (!predicate.apply(nextEvent)) {
      throw new AssertionError(messageGen.apply(nextEvent));
    }
    return nextEvent;
  }

  /**
   * Variation on {@link #assertNext(Predicate, Function)} with a constant message.
   */
  public TestingEventBuses.Event assertNext(final Predicate<TestingEventBuses.Event> predicate,
        final String message) throws InterruptedException, TimeoutException {
    return assertNext(predicate, new StaticMessage(message));
  }


  /** Similar to {@link #assertNext(Predicate, Function)} but predicate is on the value directly. */
  public <T> TestingEventBuses.Event assertNextValue(final Predicate<T> predicate ,
        Function<TestingEventBuses.Event, String> messageGen)
            throws InterruptedException, TimeoutException {
    return assertNext(new Predicate<TestingEventBuses.Event>() {
      @Override public boolean apply(@Nonnull Event input) {
        return predicate.apply(input.<T>getTypedValue());
      }
    }, messageGen);
  }

  /** Similar to {@link #assertNext(Predicate, String)} but predicate is on the value directly. */
  public <T> TestingEventBuses.Event assertNextValue(final Predicate<T> predicate, String message)
         throws InterruptedException, TimeoutException {
    return assertNext(new Predicate<TestingEventBuses.Event>() {
      @Override public boolean apply(@Nonnull Event input) {
        return predicate.apply(input.<T>getTypedValue());
      }
    }, message);
  }

  public <T> TestingEventBuses.Event assertNextValueEq(final T expected)
         throws InterruptedException, TimeoutException {
    return assertNextValue(Predicates.equalTo(expected),
        new Function<TestingEventBuses.Event, String>() {
          @Override public String apply(@Nonnull Event input) {
            return "Event value mismatch: " + input.getValue() + " != " + expected;
          }
    });
  }

  /**
   * Verify that all next several values are a permutation of the expected collection of event
   * values. This method allows for testing that certain values are produced in some random order.
   * Blocking assert.  */
  public <T> void assertNextValuesEq(final Collection<T> expected)
         throws InterruptedException, TimeoutException {
    final Set<T> remainingExpectedValues = new HashSet<>(expected);
    final Predicate<T> checkInRemainingAndRemove = new Predicate<T>() {
      @Override public boolean apply(@Nonnull T input) {
        if (! remainingExpectedValues.contains(input)) {
          return false;
        }
        remainingExpectedValues.remove(input);
        return true;
      }
    };

    while (remainingExpectedValues.size() > 0) {
      assertNextValue(checkInRemainingAndRemove,
          new Function<TestingEventBuses.Event, String>() {
            @Override public String apply(@Nonnull Event input) {
              return "Event value " + input.getValue() + " not in set " + remainingExpectedValues;
            }
      });
    }
  }

}
