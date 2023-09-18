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

package org.apache.gobblin.service.modules.orchestration;


import java.util.Optional;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;


@Alpha
@Slf4j
@AllArgsConstructor
public class NewDagManagerBoilerPlate {
  private DagTaskStream dagTaskStream;
  private DagProcFactory dagProcFactory;
  private DagManager.DagManagerThread [] dagManagerThreads;
  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;
  //TODO instantiate DMT

  @AllArgsConstructor
  public static class DagManagerThread implements Runnable {
    private DagTaskStream dagTaskStream;
    private DagProcFactory dagProcFactory;

    private MultiActiveLeaseArbiter multiActiveLeaseArbiter;
    @Override
    public void run() {
      try {
        while (dagTaskStream.hasNext()) {
          Optional<DagTask> dagTask = getNextTask();
          if (dagTask.isPresent()) {
            DagProc dagProc = dagTask.get().host(dagProcFactory);
            dagProc.process();
            //marks lease success and releases it
            dagTask.get().conclude(multiActiveLeaseArbiter);
          }
        }
      } catch (Exception ex) {
        //TODO: need to handle exceptions gracefully
        log.error(String.format("Exception encountered in %s", getClass().getName()), ex);
      }
    }
    public Optional<DagTask> getNextTask() {
      return dagTaskStream.next();
    }
    public void cleanUpDagTask() {
      throw new UnsupportedOperationException("Not yet supported");
    }
  }

  public static void submitNextDagTask() {
    throw new UnsupportedOperationException("Not yet supported");
  }
}
