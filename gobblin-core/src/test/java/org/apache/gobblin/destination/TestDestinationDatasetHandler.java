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

import java.io.IOException;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.workunit.WorkUnitStream;


public class TestDestinationDatasetHandler implements DestinationDatasetHandler {
  public static String TEST_COUNTER_KEY = "counter";
  public TestDestinationDatasetHandler(SourceState state, Boolean canCleanUp){
  }

  @Override
  public WorkUnitStream handle(WorkUnitStream workUnitSteam) {
    return workUnitSteam.transform(wu -> {
      wu.setProp(TEST_COUNTER_KEY, wu.getPropAsInt(TEST_COUNTER_KEY, 0) + 1);
      return wu;
    });
  }

  @Override
  public void close() throws IOException {}
}
