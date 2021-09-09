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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.util.JobLauncherUtils;


/**
 * Initializes and runs handlers on workunits before writers are initialized
 * Reads {@link ConfigurationKeys#DESTINATION_DATASET_HANDLER_CLASS} as a list
 * of classes, separated by comma to initialize the handlers
 */
public class DestinationDatasetHandlerService implements Closeable {
  List<DestinationDatasetHandler> handlers;

  public DestinationDatasetHandlerService(SourceState jobState, Boolean canCleanUp, EventSubmitter eventSubmitter) {
    this.handlers = new ArrayList<>();
    if (jobState.contains(ConfigurationKeys.DESTINATION_DATASET_HANDLER_CLASS)) {
      List<String> handlerList = jobState.getPropAsList(ConfigurationKeys.DESTINATION_DATASET_HANDLER_CLASS);
      for (String handlerClass : handlerList) {
        this.handlers.add(DestinationDatasetHandlerFactory.newInstance(handlerClass, jobState, canCleanUp));
      }
    }
  }

  /**
   * Executes handlers
   * @param workUnitStream
   */
  public void executeHandlers(WorkUnitStream workUnitStream) {
    if (handlers.size() > 0) {
      if (workUnitStream.isSafeToMaterialize()) {
        Collection<WorkUnit> workUnits = JobLauncherUtils.flattenWorkUnits(workUnitStream.getMaterializedWorkUnitCollection());
          for (DestinationDatasetHandler handler : this.handlers) {
            try {
              handler.handle(workUnits);
            } catch (IOException e) {
              throw new RuntimeException(String.format("Handler %s failed to execute", handler.getClass().getName()), e);
            }
          }
      } else {
        throw new RuntimeException(DestinationDatasetHandlerService.class.getName() + " does not support work unit streams");
      }
    }
  }


  public void close() throws IOException {
    for (DestinationDatasetHandler handler: this.handlers) {
      handler.close();
    }
  }
}
