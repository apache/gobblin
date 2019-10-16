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

package org.apache.gobblin.runtime.standalone;

import com.google.common.collect.Sets;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.util.ClassAliasResolver;


/**
 * Instantiates a Gobblin Standalone Service that listens to Kafka topic for jobs.
 */
@Slf4j
public class GobblinStandaloneService implements ApplicationLauncher {
  // thread used to keep process up
  private Thread idleProcessThread;

  public static void main(String[] args) {

  }

  @Override
  public void start() throws ApplicationException {
    log.info("Starting the Gobblin Cluster Manager");
    //Start the Kafka here

//    if (this.isStandaloneMode) {
//      // standalone mode starts non-daemon threads later, so need to have this thread to keep process up
//      this.idleProcessThread = new Thread(() -> {
//        while (!GobblinClusterManager.this.stopStatus.isStopInProgress() && !GobblinClusterManager.this.stopIdleProcessThread) {
//          try {
//            Thread.sleep(300);
//          } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            break;
//          }
//        }
//      });
//
//      this.idleProcessThread.start();
//
//      // Need this in case a kill is issued to the process so that the idle thread does not keep the process up
//      // since GobblinClusterManager.stop() is not called this case.
//      Runtime.getRuntime().addShutdownHook(new Thread() {
//        @Override
//        public void run() {
//          GobblinClusterManager.this.stopIdleProcessThread = true;
//        }
//      });
//    } else {
//      startAppLauncherAndServices();
//    }
  }

  @Override
  public void stop() throws ApplicationException {

  }

  @Override
  public void close() throws IOException {

  }
}
