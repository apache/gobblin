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

package org.apache.gobblin.source;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import java.io.IOException;
import java.util.List;

import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * An interface for classes that the end users implement to work with a data source from which
 * schema and data records can be extracted.
 *
 * <p>
 *   An implementation of this interface should contain all the logic required to work with a
 *   specific data source. This usually includes work determination and partitioning, and details
 *   of the connection protocol to work with the data source.
 * </p>
 *
 * @author kgoodhop
 *
 * @param <S> output schema type
 * @param <D> output record type
 */
public interface Source<S, D> {

  /**
   * Get a list of {@link WorkUnit}s, each of which is for extracting a portion of the data.
   *
   * <p>
   *   Each {@link WorkUnit} will be used instantiate a {@link org.apache.gobblin.configuration.WorkUnitState} that gets passed to the
   *   {@link #getExtractor(org.apache.gobblin.configuration.WorkUnitState)} method to get an {@link Extractor} for extracting schema
   *   and data records from the source. The {@link WorkUnit} instance should have all the properties
   *   needed for the {@link Extractor} to work.
   * </p>
   *
   * <p>
   *   Typically the list of {@link WorkUnit}s for the current run is determined by taking into account
   *   the list of {@link WorkUnit}s from the previous run so data gets extracted incrementally. The
   *   method {@link org.apache.gobblin.configuration.SourceState#getPreviousWorkUnitStates} can be used to get the list of {@link WorkUnit}s
   *   from the previous run.
   * </p>
   *
   * @param state see {@link org.apache.gobblin.configuration.SourceState}
   * @return a list of {@link WorkUnit}s
   */
  public abstract List<WorkUnit> getWorkunits(SourceState state);

  /**
   * Get an {@link Extractor} based on a given {@link org.apache.gobblin.configuration.WorkUnitState}.
   *
   * <p>
   *   The {@link Extractor} returned can use {@link org.apache.gobblin.configuration.WorkUnitState} to store arbitrary key-value pairs
   *   that will be persisted to the state store and loaded in the next scheduled job run.
   * </p>
   *
   * @param state a {@link org.apache.gobblin.configuration.WorkUnitState} carrying properties needed by the returned {@link Extractor}
   * @return an {@link Extractor} used to extract schema and data records from the data source
   * @throws IOException if it fails to create an {@link Extractor}
   */
  public abstract Extractor<S, D> getExtractor(WorkUnitState state)
      throws IOException;

  /**
   * Shutdown this {@link Source} instance.
   *
   * <p>
   *   This method is called once when the job completes. Properties (key-value pairs) added to the input
   *   {@link SourceState} instance will be persisted and available to the next scheduled job run through
   *   the method {@link #getWorkunits(SourceState)}.  If there is no cleanup or reporting required for a
   *   particular implementation of this interface, then it is acceptable to have a default implementation
   *   of this method.
   * </p>
   *
   * @param state see {@link SourceState}
   */
  public abstract void shutdown(SourceState state);

  /**
   * Instead of handling all {@link WorkUnit}s in one run, some {@link Source} may choose to stop early in order to handle the
   * proper workload, which can cause multiple runs after the initial run.
   * @return If the same job has early stopped
   */
  public default boolean isEarlyStopped() {
    return false;
  }
}
