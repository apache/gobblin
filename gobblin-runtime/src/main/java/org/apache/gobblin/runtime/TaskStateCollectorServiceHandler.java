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
import org.apache.gobblin.configuration.WorkUnitState;
import java.io.Closeable;
import java.util.Collection;


/**
 * Define basic interface for Handler in TaskStateCollectorService,
 * which runs in the gobblin's driver level.
 *
 */
public interface TaskStateCollectorServiceHandler extends Closeable {

  /**
   * Interface of handler factory.
   */
  interface TaskStateCollectorServiceHandlerFactory {
    TaskStateCollectorServiceHandler createHandler(JobState jobState);
  }

  /**
   * Execute the actions of handler.
   */
  public void handle(Collection<? extends WorkUnitState> states) throws IOException;
}
