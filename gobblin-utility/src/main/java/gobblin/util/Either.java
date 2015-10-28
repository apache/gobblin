/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import lombok.AllArgsConstructor;

import com.google.common.base.Optional;


/**
 * Created by ibuenros on 10/29/15.
 */
@AllArgsConstructor
public class Either<S, T> {

  protected final Optional<S> s;
  protected final Optional<T> t;

  public static class Left<S, T> extends Either<S, T> {
    public Left(S s) {
      super(Optional.of(s), Optional.<T>absent());
    }

    public S getLeft() {
      return this.s.get();
    }
  }

  public static class Right<S, T> extends Either<S, T> {
    public Right(T t) {
      super(Optional.<S>absent(), Optional.of(t));
    }

    public T getRight() {
      return this.t.get();
    }
  }

  public static <S, T> Either<S, T> left(S left) {
    return new Left<S, T>(left);
  }

  public static <S, T> Either<S, T> right(T right) {
    return new Right<S, T>(right);
  }

  public Object get() {
    if(this.s.isPresent()) {
      return this.s.get();
    } else {
      return this.t.get();
    }
  }

}
