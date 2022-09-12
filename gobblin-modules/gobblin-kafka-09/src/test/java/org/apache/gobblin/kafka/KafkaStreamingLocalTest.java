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

package org.apache.gobblin.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.gobblin.writer.test.GobblinTestEventBusWriter;
import org.apache.gobblin.writer.test.TestingEventBusAsserter;
import org.testng.annotations.Test;


public class KafkaStreamingLocalTest {
  //disable the test as streaming task will never end unless manually kill it
  @Test(enabled=false)
  public void testStreamingLocally() {
    String eventBusId = this.getClass().getName() + ".jobFileTest";

    TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId);

    EmbeddedGobblin embeddedGobblin =
        new EmbeddedGobblin("TestJob").jobFile(this.getClass().getClassLoader().getResource("kafkaHdfsStreaming.conf").getPath());
    embeddedGobblin.setConfiguration(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY, eventBusId);

    try {
      JobExecutionResult result = embeddedGobblin.run();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }
}
