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

package gobblin.broker;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;

import com.google.common.util.concurrent.Service;

import gobblin.broker.iface.ScopeType;


/**
 * General utilities for {@link gobblin.broker.iface.SharedResourcesBroker} functionality.
 */
public class SharedResourcesBrokerUtils {

  /**
   * Determine if a {@link ScopeType} is an ancestor of another {@link ScopeType}.
   */
  public static <S extends ScopeType<S>> boolean isScopeTypeAncestor(S scopeType, S possibleAncestor) {
    Queue<S> ancestors = new LinkedList<>();
    ancestors.add(scopeType);
    while (true) {
      if (ancestors.isEmpty()) {
        return false;
      }
      if (ancestors.peek().equals(possibleAncestor)) {
        return true;
      }
      Collection<S> parentScopes = ancestors.poll().parentScopes();
      if (parentScopes != null) {
        ancestors.addAll(parentScopes);
      }
    }
  }

  /**
   * Determine if a {@link ScopeWrapper} is an ancestor of another {@link ScopeWrapper}.
   */
  static <S extends ScopeType<S>> boolean isScopeAncestor(ScopeWrapper<S> scope, ScopeWrapper<S> possibleAncestor) {
    Queue<ScopeWrapper<S>> ancestors = new LinkedList<>();
    ancestors.add(scope);
    while (true) {
      if (ancestors.isEmpty()) {
        return false;
      }
      if (ancestors.peek().equals(possibleAncestor)) {
        return true;
      }
      ancestors.addAll(ancestors.poll().getParentScopes());
    }
  }

  /**
   * Close {@link Closeable}s and shutdown {@link Service}s.
   */
  public static void shutdownObject(Object obj, Logger log) {
    if (obj instanceof Service) {
      ((Service) obj).stopAsync();
    } else if (obj instanceof Closeable) {
      try {
        ((Closeable) obj).close();
      } catch (IOException ioe) {
        log.error("Failed to close {}.", obj);
      }
    }
  }
}
