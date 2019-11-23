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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.HiveRegistrationPublisher;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A {@link TaskStateCollectorServiceHandler} implementation that execute hive registration on driver level.
 * It registers all {@link TaskState} once they are available.
 * Since {@link TaskStateCollectorService} is by default being invoked every minute,
 * if a single batch of hive registration finishes within a minute, the latency can be hidden by the gap between two run
 * of {@link TaskStateCollectorService}.
 */
@Slf4j
public class HiveRegTaskStateCollectorServiceHandlerImpl implements TaskStateCollectorServiceHandler {

  private static final String TASK_COLLECTOR_SERVICE_PREFIX = "task.collector.service";
  private static final String HIVE_REG_PUBLISHER_CLASS = "hive.reg.publisher.class";
  private static final String HIVE_REG_PUBLISHER_CLASS_KEY = TASK_COLLECTOR_SERVICE_PREFIX + "." + HIVE_REG_PUBLISHER_CLASS;
  private static final String DEFAULT_HIVE_REG_PUBLISHER_CLASS =
      "org.apache.gobblin.publisher.HiveRegistrationPublisher";
  private HiveRegistrationPublisher hiveRegHandler;

  public HiveRegTaskStateCollectorServiceHandlerImpl(JobState jobState) {
    String className = jobState
        .getProp(HIVE_REG_PUBLISHER_CLASS_KEY, DEFAULT_HIVE_REG_PUBLISHER_CLASS);
      try {
        hiveRegHandler = (HiveRegistrationPublisher) GobblinConstructorUtils.invokeLongestConstructor(Class.forName(className), jobState);
      }catch (ReflectiveOperationException e) {
        throw new RuntimeException("Could not instantiate HiveRegistrationPublisher " + className, e);
      }
  }

  @Override
  public void handle(Collection<? extends WorkUnitState> taskStates)
      throws IOException {
    this.hiveRegHandler.publishData(taskStates);
  }

  @Override
  public void close()
      throws IOException {
    hiveRegHandler.close();
  }
}
