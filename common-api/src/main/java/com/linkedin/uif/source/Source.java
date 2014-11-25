/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.source;

import java.io.IOException;
import java.util.List;

import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * <p>Primary plugin point for end users.  A Source implementation should contain all the logic
 * required for a specific data source.  This usually includes work determination and 
 * connection protocols</p>
 * 
 * @author kgoodhop
 *
 * @param <S> output schema type
 * @param <D> output record type
 */
public interface Source<S, D> {
  
  /**
   * <p>Returns a list of {@link WorkUnit}.  Each {@link WorkUnit} will be used instantiate
   * a {@link WorkUnitState} and passed to {@link #getExtractor(WorkUnitState)} method.  The 
   * {@link WorkUnit} instance should have all the properties needed by {@link Extractor}
   * </p>
   * 
   * @param state see {@link SourceState}
   * @return
   */
  public abstract List<WorkUnit> getWorkunits(SourceState state);

  /**
   * <p>
   * Returns the {@link Extractor} instance responsible for doing the actual pulling of
   * the data.  The {@link Extractor} can use {@link WorkUnitState} for storing values that
   * will be persisted to the next scheduled run.
   * </p>
   * 
   * @param state
   * @return
   * @throws IOException 
   */
  public abstract Extractor<S, D> getExtractor(WorkUnitState state) throws IOException;
  
  /**
   * <p>Called once when the pull job has completed.  Properties added to this instance of
   * {@link SourceState} will be persisted and available to the next scheduled run through the
   * call to {@link #getWorkunits(SourceState)}.  If there is no cleanup or reporting required 
   * for a particular implementation of this interface, then it is acceptable to provided a 
   * default implementation of this method.
   * </p>
   * 
   * @param state see {@link SourceState}
   */
  public abstract void shutdown(SourceState state);
}
