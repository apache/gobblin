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

package org.apache.gobblin.initializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.io.Closer;


/**
 * Wraps multiple writer initializers, which is useful when more than one branch.
 */
@ToString
public class MultiInitializer implements Initializer {

  /** Commemorate each (`Optional`) {@link org.apache.gobblin.initializer.Initializer.AfterInitializeMemento} of every wrapped {@link Initializer} */
  @Data
  @Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
  @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
  @RequiredArgsConstructor
  private static class Memento implements AfterInitializeMemento {
    // WARNING: not possible to use `List<Optional<AfterInitializeMemento>>`, as first attempted, due to:
    //   com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: Unrecognized field "present" (class java.util.Optional), not marked as
    //   ignorable (0 known properties: ])
    //     at [Source:(String)"{\"@class\":\"org.apache.gobblin.initializer.MultiInitializer$Memento\",\"orderedInitializersMementos\":[{\"present\":false}]}"]
    //     (through reference chain: org.apache.gobblin.initializer.MultiInitializer$Memento[\"orderedInitializersMementos\"]->java.util.ArrayList[0]
    //       ->java.util.Optional[\"present\"])",
    // the following does NOT fix, probably due to `Optional`'s nesting with `List`:
    //   @JsonIgnoreProperties(ignoreUnknown = true)
    @NonNull private List<AfterInitializeMemento> orderedInitializersMementos;
  }


  private final List<Initializer> initializers;
  private final Closer closer;

  public MultiInitializer(List<? extends Initializer> initializers) {
    this.initializers = ImmutableList.copyOf(initializers);
    this.closer = Closer.create();
    for (Initializer initializer : this.initializers) {
      this.closer.register(initializer);
    }
  }

  @Override
  public void initialize() {
    for (Initializer initializer : this.initializers) {
      initializer.initialize();
    }
  }

  @Override
  public void close() {
    try {
      this.closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<AfterInitializeMemento> commemorate() {
    return Optional.of(new MultiInitializer.Memento(this.initializers.stream()
        .map(Initializer::commemorate)
        .map(opt -> opt.orElse(null))
        .collect(Collectors.toList())));
  }

  @Override
  public void recall(AfterInitializeMemento memento) {
    Memento recollection = memento.castAsOrThrow(MultiInitializer.Memento.class, this);
    Streams.zip(this.initializers.stream(), recollection.orderedInitializersMementos.stream(), (initializer, nullableInitializerMemento) -> {
      Optional.ofNullable(nullableInitializerMemento).ifPresent(initializer::recall);
      return null;
    }).count(); // force evaluation, since `Streams.zip` used purely for side effects
  }
}