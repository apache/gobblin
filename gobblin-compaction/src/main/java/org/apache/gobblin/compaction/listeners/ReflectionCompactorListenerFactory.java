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

package org.apache.gobblin.compaction.listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import org.apache.gobblin.configuration.State;


/**
 * Implementation of {@link CompactorListenerFactory} that creates a {@link CompactorListener} using reflection. The
 * config key {@link #COMPACTOR_LISTENERS} is used to specify a comma separated list of compactors to use. These
 * compactors will be run serially.
 */
public class ReflectionCompactorListenerFactory implements CompactorListenerFactory {

  @VisibleForTesting
  static final String COMPACTOR_LISTENERS = "compactor.listeners";

  @Override
  public Optional<CompactorListener> createCompactorListener(Properties properties)
      throws CompactorListenerCreationException {
    State state = new State(properties);

    if (Strings.isNullOrEmpty(state.getProp(COMPACTOR_LISTENERS))) {
      return Optional.absent();
    }

    List<CompactorListener> listeners = new ArrayList<>();
    for (String listenerClassName : state.getPropAsList(COMPACTOR_LISTENERS)) {
      try {
        listeners.add((CompactorListener) ConstructorUtils
            .invokeConstructor(Class.forName(listenerClassName), properties));
      } catch (ReflectiveOperationException e) {
        throw new CompactorListenerCreationException(String
            .format("Unable to create CompactorListeners from key \"%s\" with value \"%s\"", COMPACTOR_LISTENERS,
                properties.getProperty(COMPACTOR_LISTENERS)), e);
      }
    }
    return Optional.<CompactorListener>of(new SerialCompactorListener(listeners));
  }
}
