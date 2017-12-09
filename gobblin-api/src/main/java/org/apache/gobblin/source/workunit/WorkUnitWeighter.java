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

package org.apache.gobblin.source.workunit;

/**
 * Provides weights for {@link WorkUnit}s to use by a {@link WorkUnitBinPacker}.
 *
 * <p>
 *   The weight is used by a bin packing algorithm to organize {@link WorkUnit}s into {@link org.apache.gobblin.source.workunit.MultiWorkUnit}s
 *   with a bounded total weight. The weighter must have the following properties:
 *   <ul>
 *     <li>If wu1.equals(wu2), then weight(wu1) == weight(wu2).</li>
 *     <li>Each weight must be positive.</li>
 *   </ul>
 *   Ideally, the weights are a linear representation of the resources needed to process a work unit.
 * </p>
 */
public interface WorkUnitWeighter {

  /**
   * The weight of this work unit.
   */
  public long weight(WorkUnit workUnit);

}
