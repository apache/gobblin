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
import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * Alternative to {@link Function} that handles wrapping (or tunneling) a single checked {@link Exception} derived class.
 * Inspired by: https://dzone.com/articles/how-to-handle-checked-exception-in-lambda-expressi
 */
@FunctionalInterface
public interface CheckedExceptionFunction<T, R, E extends Exception> {
  /**
   * Wrapper to tunnel {@link IOException} as an unchecked exception that would later be unwrapped via
   * {@link WrappedIOException#rethrowWrapped()}.  If no expectation of unwrapping, this wrapper may simply add
   * unnecessar obfuscation: instead use {@link CheckedExceptionFunction#wrapToUnchecked(CheckedExceptionFunction)}
   *
   * BUMMER: specific {@link IOException} hard-coded because: "generic class may not extend 'java.lang.Throwable'"
   */
  @RequiredArgsConstructor
  public static class WrappedIOException extends RuntimeException {
    @Getter
    private final IOException wrappedException;

    /** CAUTION: if this be your intent, DO NOT FORGET!  Being unchecked, the compiler WILL NOT remind you. */
    public void rethrowWrapped() throws IOException {
      throw wrappedException;
    }
  }

  R apply(T arg) throws E;

  /** @return {@link Function} proxy that catches any instance of {@link Exception} and rethrows it wrapped as {@link RuntimeException} */
  static <T, R, E extends Exception> Function<T, R> wrapToUnchecked(CheckedExceptionFunction<T, R, E> f) {
    return a -> {
      try {
        return f.apply(a);
      } catch (RuntimeException re) {
        throw re; // no double wrapping
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * @return {@link Function} proxy that catches any instance of {@link IOException}, and rethrows it wrapped as {@link WrappedIOException},
   * for easy unwrapping via {@link WrappedIOException#rethrowWrapped()}
   */
  static <T, R, E extends IOException> Function<T, R> wrapToTunneled(CheckedExceptionFunction<T, R, E> f) {
    return a -> {
      try {
        return f.apply(a);
      } catch (RuntimeException re) {
        throw re; // no double wrapping
      } catch (IOException ioe) {
        throw new WrappedIOException(ioe);
      }
    };
  }
}
