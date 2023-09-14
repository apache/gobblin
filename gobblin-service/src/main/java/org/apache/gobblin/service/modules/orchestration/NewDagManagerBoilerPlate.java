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

import com.google.common.base.Optional;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@WorkInProgress
@Slf4j
@AllArgsConstructor
public class NewDagManagerBoilerPlate {
  private DagTaskStream dagTaskStream;
  private DagProcFactory dagProcFactory;
  private DagManager.DagManagerThread [] dagManagerThreads;
  //TODO instantiate DMT

  @WorkInProgress
  @AllArgsConstructor
  public static class DagManagerThread implements Runnable {
    private DagTaskStream dagTaskStream;
    private DagProcFactory dagProcFactory;
    @Override
    public void run() {
      try {
        while (dagTaskStream.hasNext()) {
          Optional<DagTask> dagTask = getNextTask();
          if (dagTask.isPresent()) {
            DagProc dagProc = dagTask.get().host(dagProcFactory);
            dagProc.process(dagTask.get().leaseAttemptStatus);
            //TODO: Handle cleaning up of Dags
            cleanUpDagTask();
          }
        }
      } catch (Exception ex) {
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
