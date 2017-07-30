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

import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.EventBus;

import lombok.Getter;
import lombok.ToString;

/**
 * Maintains a static set of EventBus instances for testing purposes by
 * {@link GobblinTestEventBusWriter}. Obviously, this class should be used only in test VMs with
 * limited life span.
 */
public class TestingEventBuses {
  private static final LoadingCache<String, EventBus> _instances =
      CacheBuilder.newBuilder().build(new CacheLoader<String, EventBus>(){
        @Override public EventBus load(String key) throws Exception {
          return new EventBus(key);
        }
      });

  public static EventBus getEventBus(String eventBusId) {
    try {
      return _instances.get(eventBusId);
    } catch (ExecutionException e) {
      throw new RuntimeException("Unable to create an EventBus with id " + eventBusId + ": " + e, e);
    }
  }

  @Getter
  @ToString
  public static class Event {
    private final Object value;
    private final long timestampNanos;

    public Event(Object value) {
      this.value = value;
      this.timestampNanos  = System.nanoTime();
    }

    @SuppressWarnings("unchecked")
    public <T> T getTypedValue() {
      return (T)this.value;
    }

    public boolean valueEquals(Object otherValue) {
      if (null == this.value) {
        return null == otherValue;
      }
      return this.value.equals(otherValue);
    }
  }
}
