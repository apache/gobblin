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

package org.apache.gobblin.fork;

import java.io.Closeable;
import java.util.List;

import org.apache.gobblin.configuration.WorkUnitState;


/**
 * An interface for fork operators that convert one input data record into multiple
 * records. So essentially this operator forks one input data stream into multiple
 * data streams. This interface allows user to plugin their fork logic.
 *
 * @author Yinan Li
 *
 * @param <S> schema data type
 * @param <D> data record data type
 */
public interface ForkOperator<S, D> extends Closeable {

  /**
   * Initialize this {@link ForkOperator}.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   */
  public void init(WorkUnitState workUnitState)
      throws Exception;

  /**
   * Get the number of branches after the fork.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @return number of branches after the fork
   */
  public int getBranches(WorkUnitState workUnitState);

  /**
   * Get a list of {@link java.lang.Boolean}s indicating if the schema should go to each branch.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @param input input schema
   * @return list of {@link java.lang.Boolean}s
   */
  public List<Boolean> forkSchema(WorkUnitState workUnitState, S input);

  /**
   * Get a list of {@link java.lang.Boolean}s indicating if the record should go to each branch.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @param input input data record
   * @return list of {@link java.lang.Boolean}s
   */
  public List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input);
}
