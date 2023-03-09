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

package org.apache.gobblin.util.function;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * Alternative to {@link Predicate} that handles wrapping (or tunneling) a single checked {@link Exception} derived class.
 * Based on and extremely similar to {@link CheckedExceptionFunction}.<br><br>
 *
 * At first glance, it appears these 2 classes could be generalized and combined without the uncomfortable amount of duplication.
 * But this is not possible to do cleanly because:
 *   <ul>
 *     <li> {@link Predicate} and {@link Function} are separate types with no inheritance hierarchy relationship</li>
 *     <li>
 *       {@link CheckedExceptionPredicate#wrapToUnchecked(CheckedExceptionPredicate)} returns a {@link Predicate}
 *       but {@link CheckedExceptionFunction#wrapToUnchecked(CheckedExceptionFunction)} returns a {@link Function}. And
 *       since Java does not support higher level generics / type classes (i.e. type parameters for types that are
 *       themselves parameterized)
 *     </li>
 *   </ul>
 */
@FunctionalInterface
public interface CheckedExceptionPredicate<T, E extends Exception> {
  /**
   * Wrapper to tunnel {@link IOException} as an unchecked exception that would later be unwrapped via
   * {@link WrappedIOException#rethrowWrapped()}.  If no expectation of unwrapping, this wrapper may simply add
   * unnecessary obfuscation: instead use {@link CheckedExceptionPredicate#wrapToUnchecked(CheckedExceptionPredicate)}
   *
   * BUMMER: specific {@link IOException} hard-coded because: "generic class may not extend {@link java.lang.Throwable}"
   */
  @RequiredArgsConstructor
  class WrappedIOException extends RuntimeException {
    @Getter
    private final IOException wrappedException;

    /** CAUTION: if this be your intent, DO NOT FORGET!  Being unchecked, the compiler WILL NOT remind you. */
    public void rethrowWrapped() throws IOException {
      throw wrappedException;
    }
  }

  boolean test(T arg) throws E;

  /** @return {@link Predicate} proxy that catches any instance of {@link Exception} and rethrows it wrapped as {@link RuntimeException} */
  static <T,  E extends Exception> Predicate<T> wrapToUnchecked(CheckedExceptionPredicate<T, E> f) {
    return a -> {
      try {
        return f.test(a);
      } catch (RuntimeException re) {
        throw re; // no double wrapping
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * @return {@link Predicate} proxy that catches any instance of {@link IOException}, and rethrows it wrapped as {@link WrappedIOException},
   * for easy unwrapping via {@link WrappedIOException#rethrowWrapped()}
   */
  static <T, E extends IOException> Predicate<T> wrapToTunneled(CheckedExceptionPredicate<T, E> f) {
    return a -> {
      try {
        return f.test(a);
      } catch (RuntimeException re) {
        throw re; // no double wrapping
      } catch (IOException ioe) {
        throw new WrappedIOException(ioe);
      }
    };
  }
}
