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

package gobblin.runtime;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

import gobblin.publisher.HiveRegistrationPublisher;

import lombok.extern.slf4j.Slf4j;

import static gobblin.configuration.ConfigurationKeys.*;

@Slf4j
public class TaskStateCollectorHandlerFactory {

  /**
   * Supported type of TastStateCollectorHanlder
   */
  public enum TaskStateCollectorHandlerType {
    HIVEREG,
    DUMMY
  }

  public static Closeable newCloseableHanlder(JobState jobState){
    Properties jobProps = jobState.getProperties();

    Optional<TaskStateCollectorHandlerType> handlerType = Enums.getIfPresent(TaskStateCollectorHandlerType.class,
        jobProps.containsKey(TASK_STATE_COLLECTOR_HANDLER_CLASS)? jobProps.getProperty(
            TASK_STATE_COLLECTOR_HANDLER_CLASS):"DEFAULT");

    if (handlerType.isPresent()) {
      switch ((handlerType.get())) {
        case HIVEREG:
          return new HiveRegistrationPublisher(jobState);
        case DUMMY:
          return new Closeable() {
            @Override
            public void close() throws IOException {
              log.info("A dummy TaskStateCollectorHandler, do nothing");
            }
          };
        default:
          return null;
      }
    }
    else{
      return null;
    }
  }
}
