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

package gobblin.util;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import com.google.common.base.Optional;


/**
 * Represents a value of one of two possible types. Similar to Scala's Either.
 * Each instance of {@link Either} is of type {@link Left} or {@link Right} depending of the class of its value.
 *
 * <p>
 *   This class is useful when a class has two different states, each of which depends on a variable of a different
 *   type (for example a String name or Numeric id). While the normal pattern is to have two variables, only one of
 *   which is set, or two Optionals, only one of which is present, that pattern does not protect from having none
 *   or both variables set by programming errors. Instead, a single instance of Either can be used, which guarantees
 *   exactly one type will be set.
 * </p>
 *
 * @param <S> first possible type for the value.
 * @param <T> second possible type for the value.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Either<S, T> {

  protected final Optional<S> s;
  protected final Optional<T> t;

  /**
   * An instance of {@link Either} with value of type {@link S}.
   */
  public static class Left<S, T> extends Either<S, T> {
    public Left(S s) {
      super(Optional.fromNullable(s), Optional.<T>absent());
    }

    /**
     * @return value of type {@link S}.
     */
    public S getLeft() {
      return this.s.orNull();
    }
  }

  /**
   * An instance of {@link Either} with value of type {@link T}.
   */
  public static class Right<S, T> extends Either<S, T> {
    public Right(T t) {
      super(Optional.<S>absent(), Optional.fromNullable(t));
    }

    /**
     * @return value of type {@link T}.
     */
    public T getRight() {
      return this.t.orNull();
    }
  }

  /**
   * Create an instance of {@link Either} with value of type {@link S}.
   * @param left value of this instance.
   * @return an instance of {@link Left}.
   */
  public static <S, T> Either<S, T> left(S left) {
    return new Left<>(left);
  }

  /**
   * Create an instance of {@link Either} with value of type {@link T}.
   * @param right value of this instance.
   * @return an instance of {@link Right}.
   */
  public static <S, T> Either<S, T> right(T right) {
    return new Right<>(right);
  }

  /**
   * Get the value of this {@link Either} instance, returned as class {@link Object}. To get a strongly typed return,
   * check the specific type of {@link Either} and call {@link Left#getLeft} or {@link Right#getRight}.
   * @return value as an instance of {@link Object}.
   */
  public Object get() {
    if (this instanceof Left) {
      return this.s.orNull();
    }
    return this.t.orNull();
  }

}
