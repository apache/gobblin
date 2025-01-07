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

import java.util.Arrays;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/** Test {@link MultiInitializer} */
public class MultiInitializerTest {
  /** Concrete Initializer A - implements `AfterInitializeMemento` */
  private static class InitializerImplA implements Initializer {
    private static int instanceCounter = 0;

    @Data
    @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
    @RequiredArgsConstructor
    private static class Memento implements Initializer.AfterInitializeMemento {
      @NonNull private String state;
    }

    @Getter private String state;

    @Override
    public void initialize() {
      this.state = "initialized-" + (++instanceCounter);
    }

    @Override
    public void close() {
      // noop
    }

    @Override
    public Optional<AfterInitializeMemento> commemorate() {
      return Optional.of(new Memento(this.state));
    }

    @Override
    public void recall(AfterInitializeMemento memento) {
      Memento recollection = memento.castAsOrThrow(Memento.class, this);
      this.state = recollection.getState();
    }
  }


  /** Concrete Initializer B - DOES NOT implement `AfterInitializeMemento`! */
  private static class InitializerImplBNoMemento implements Initializer {
    private static int instanceCounter = 0;

    @Getter private String state;

    @Override
    public void initialize() {
      this.state = "ignore-" + (++instanceCounter);
    }

    @Override
    public void close() {
      // noop
    }
  }


  /** Concrete Initializer C - implements `AfterInitializeMemento` */
  private static class InitializerImplC implements Initializer {
    private static int instanceCounter = 0;

    @Data
    @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
    @RequiredArgsConstructor
    private static class MyMemento implements AfterInitializeMemento {
      @NonNull private int state;
    }

    @Getter private int state;

    @Override
    public void initialize() {
      this.state = 41 + (++instanceCounter);
    }

    @Override
    public void close() {
      // noop
    }

    @Override
    public Optional<AfterInitializeMemento> commemorate() {
      return Optional.of(new MyMemento(this.state));
    }

    @Override
    public void recall(AfterInitializeMemento memento) {
      MyMemento recollection = memento.castAsOrThrow(MyMemento.class, this);
      this.state = recollection.getState();
    }
  }


  @Test
  public void testMementoCommemorateToSerializeAndDeserializeForRecall() {
    // create "first generation" of concrete initializers
    InitializerImplA initializerA_1 = new InitializerImplA();
    InitializerImplBNoMemento initializerB_1 = new InitializerImplBNoMemento();
    InitializerImplC initializerC_1 = new InitializerImplC();

    // create the 1st-gen `MultiInitializer`; `initialize` all wrapped initializers
    MultiInitializer multiInitializer1G = new MultiInitializer(Arrays.asList(initializerA_1, initializerB_1, initializerC_1));
    multiInitializer1G.initialize();

    // `commemorate` and `serialize` 1st-gen state
    Optional<Initializer.AfterInitializeMemento> optMemento1G = multiInitializer1G.commemorate();
    Assert.assertTrue(optMemento1G.isPresent());
    String serializedMemento = Initializer.AfterInitializeMemento.serialize(optMemento1G.get());

    // create a new 2nd-gen `MultiInitializer` with a "second generation" of concrete initializers... but DO NOT `initialize` them!
    InitializerImplA initializerA_2 = new InitializerImplA();
    InitializerImplBNoMemento initializerB_2 = new InitializerImplBNoMemento();
    InitializerImplC initializerC_2 = new InitializerImplC();
    MultiInitializer multiInitializer2G = new MultiInitializer(Arrays.asList(initializerA_2, initializerB_2, initializerC_2));

    // verify that state differs between 1st-gen and 2nd-gen `Initializer`s
    Assert.assertNotEquals(initializerA_1.getState(), initializerA_2.getState());
    Assert.assertNotEquals(initializerB_1.getState(), initializerB_2.getState());
    Assert.assertNotEquals(initializerC_1.getState(), initializerC_2.getState());

    try {
      // verify not possible to `commemorate` prior to `recall()`
      multiInitializer2G.commemorate();
      Assert.fail("`commemorate()` somehow possible even before `Initializer.initialize()` or `recall()` - despite `@NotNull` annotation on `state`");
    } catch (NullPointerException npe) {
      // expected
    }

    // next, `deserialize` 1st-gen state and `recall` it to the (un-`initialize`d) 2nd-gen `MultiInitializer`
    Initializer.AfterInitializeMemento deserializedMemento = Initializer.AfterInitializeMemento.deserialize(serializedMemento);
    multiInitializer2G.recall(deserializedMemento);
    Optional<Initializer.AfterInitializeMemento> optMemento2G = multiInitializer2G.commemorate();
    Assert.assertTrue(optMemento2G.isPresent());

    // verify that post-`recall`ed memento equivalent to post-`initialize`d one
    Assert.assertEquals(optMemento1G.get(), optMemento2G.get());

    // WARNING: in real code, DO NOT `initialize` following `recall`, as it would reset the state of the wrapped initializers, negating the `recall`
    multiInitializer2G.initialize();
    Optional<Initializer.AfterInitializeMemento> optMemento2G_alt = multiInitializer2G.commemorate();
    Assert.assertTrue(optMemento2G_alt.isPresent());
    // verify not simply the case that mementos always equal
    Assert.assertNotEquals(optMemento2G.get(), optMemento2G_alt.get());
  }
}
