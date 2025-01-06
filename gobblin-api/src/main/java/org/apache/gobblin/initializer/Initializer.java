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

import java.io.Closeable;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public interface Initializer extends Closeable {

  /**
   * Marker interface to convey an opaque snapshot of the internal state of any concrete {@link Initializer}, thus affording state serialization for
   * eventual "revival" as a new `Initializer` holding equivalent internal state.  {@link #commemorate()} the memento after {@link #initialize()}
   * and subsequently {@link #recall(AfterInitializeMemento)} before {@link #close()}ing it.
   *
   * Whereas the "Initializer Lifecycle", when synchronous and with the same instance, is:
   *   [concrete `? implements Initializer` instance A]  `.initialize()` -> DO PROCESSING -> `.close()`
   * When using `AfterInitializer` across instances and even memory-space boundaries it becomes:
   *   [concrete `T0 implements Initializer` instance A] `.initialize()` -> `.commemorate()` -> PERSIST/TRANSMIT MEMENTO
   *       -> DO PROCESSING ->
   *   [concrete `T0 implements Initializer` instance B] RECEIVE MEMENTO -> `.recall()` -> `.close()`
   *
   * For both backwards compatibility and because not every concrete `Initializer` has internal state worth capturing, not every `Initializer`
   * impl will implement an `AfterInitializeMemento`.  Those that do will supply a unique impl cultivating self-aware impl details of their
   * `Initializer`.  An `AfterInitializeMemento` impl needs simply be (de)serializable by {@link ObjectMapper}.  An `Initializer` impl with an
   * `AfterInitializeMemento` impl MUST NOT (re-)process any {@link org.apache.gobblin.source.workunit.WorkUnit}s during its {@link #close()}
   * method: `WorkUnit` processing MUST proceed entirely within {@link #initialize()}.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class") // to handle variety of concrete impls
  public interface AfterInitializeMemento {
    static Logger logger = LoggerFactory.getLogger(AfterInitializeMemento.class);

    /**
     * Convey attempt to work with a concrete {@link AfterInitializeMemento} of type other than the single expected companion type known to `forInitializer`.
     * @see #castAsOrThrow(Class, Initializer)
     */
    static class MismatchedMementoException extends RuntimeException {
      public MismatchedMementoException(AfterInitializeMemento memento, Class<?> asClass, Initializer forInitializer) {
        super(String.format("Memento '%s' for Initializer '%s' of class '%s' - NOT '%s'", memento, forInitializer.getClass().getName(),
            memento.getClass().getName(), asClass.getName()));
      }
    }

    /** stringify as JSON */
    static String serialize(AfterInitializeMemento memento) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        String result = objectMapper.writeValueAsString(memento);
        logger.info("Serializing AfterInitializeMemento {} as '{}'", memento, result);
        return result;
      } catch (JsonProcessingException e) {
        logger.error("Failed to serialize AfterInitializeMemento '" + memento + "'", e);
        throw new RuntimeException(e);
      }
    }

    /** destringify JSON */
    static AfterInitializeMemento deserialize(String serialized) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        AfterInitializeMemento result = objectMapper.readValue(serialized, AfterInitializeMemento.class);
        logger.info("Deserializing AfterInitializeMemento '{}' as {}", serialized, result);
        return result;
      } catch (JsonProcessingException e) {
        logger.error("Failed to deserialize AfterInitializeMemento '" + serialized + "'", e);
        throw new RuntimeException(e);
      }
    }

    /** cast `this` concrete `AfterInitializeMemento` to `castClass`, else {@link MismatchedMementoException} */
    default <T extends AfterInitializeMemento> T castAsOrThrow(Class<T> castClass, Initializer forInitializer)
        throws MismatchedMementoException {
      if (castClass.isAssignableFrom(this.getClass())) {
        return (T) this;
      } else {
        throw new AfterInitializeMemento.MismatchedMementoException(this, castClass, forInitializer);
      }
    }
  }

  /**
   * Initialize the writer/converter (e.g. using the state and/or {@link org.apache.gobblin.source.workunit.WorkUnit}s provided when constructing the instance)
   */
  public void initialize();

  /**
   * Removed checked exception.
   * {@inheritDoc}
   * @see java.io.Closeable#close()
   *
   * NOTE: An `Initializer` impl with an `AfterInitializeMemento` impl MUST NOT (re-)process any {@link org.apache.gobblin.source.workunit.WorkUnit}s
   * during its {@link #close()} method: `WorkUnit` processing MUST proceed entirely within {@link #initialize()}.
   */
  @Override
  public void close();

  /** @return any `Initializer`-specific companion memento, as required to convey internal state after {@link #initialize()}, as needed to {@link #close()} */
  default Optional<AfterInitializeMemento> commemorate() {
    return Optional.empty();
  }

  /**
   * "reinitialize" a fresh instance with (equiv.) post {@link #initialize()} internal state, per `Initializer`-specific companion `memento`
   * to {@link #close()}
   */
  default void recall(AfterInitializeMemento memento) {
    // noop
  }
}
