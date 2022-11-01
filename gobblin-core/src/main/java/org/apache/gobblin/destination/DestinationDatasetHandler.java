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

package org.apache.gobblin.destination;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;


/**
 * Performs work related to initializing the target environment before the files are written and published.
 * Implementations should be aware that a {@link WorkUnitStream} may be of streaming type.
 */
public interface DestinationDatasetHandler extends Closeable {

  /**
   * Handle destination setup before workunits are sent to writer and publisher
   * This method is deprecated in favor of {@link #handle(WorkUnitStream)}.
   * @param workUnits
   */
  @Deprecated
  default void handle(Collection<WorkUnit> workUnits) throws IOException {}

  default WorkUnitStream handle(WorkUnitStream workUnitStream) throws IOException {
    return workUnitStream;
  }

  /**
   * Perform cleanup if needed
   * @throws IOException
   */
  void close() throws IOException;
}
