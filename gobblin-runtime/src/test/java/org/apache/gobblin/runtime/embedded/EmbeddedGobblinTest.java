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

package org.apache.gobblin.runtime.embedded;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.template.ResourceBasedJobTemplate;
import org.apache.gobblin.util.test.HelloWorldSource;
import org.apache.gobblin.writer.test.GobblinTestEventBusWriter;
import org.apache.gobblin.writer.test.TestingEventBusAsserter;


public class EmbeddedGobblinTest {

  @Test
  public void testRunWithTemplate() throws Exception {
    String eventBusId = this.getClass().getName();
    int numHellos = 5;

    TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId);

    EmbeddedGobblin embeddedGobblin =
        new EmbeddedGobblin("TestJob").setTemplate(ResourceBasedJobTemplate.forResourcePath("templates/hello-world.template"));
    embeddedGobblin.setConfiguration(ConfigurationKeys.WRITER_BUILDER_CLASS, GobblinTestEventBusWriter.Builder.class.getName());
    embeddedGobblin.setConfiguration(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY, eventBusId);
    embeddedGobblin.setConfiguration(HelloWorldSource.NUM_HELLOS_FULL_KEY, Integer.toString(numHellos));
    JobExecutionResult result = embeddedGobblin.run();

    Assert.assertTrue(result.isSuccessful());

    ArrayList<String> expectedEvents = new ArrayList<>();
    for (int i = 1; i <= numHellos; ++i) {
      expectedEvents.add(HelloWorldSource.ExtractorImpl.helloMessage(i));
    }
    asserter.assertNextValuesEq(expectedEvents);
    asserter.close();
  }

  @Test
  public void testRunWithJobFile() throws Exception {
    String eventBusId = this.getClass().getName() + ".jobFileTest";

    TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId);

    EmbeddedGobblin embeddedGobblin =
        new EmbeddedGobblin("TestJob").jobFile(getClass().getResource("/testJobs/helloWorld.conf").getPath());
    embeddedGobblin.setConfiguration(ConfigurationKeys.WRITER_BUILDER_CLASS, GobblinTestEventBusWriter.Builder.class.getName());
    embeddedGobblin.setConfiguration(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY, eventBusId);

    JobExecutionResult result = embeddedGobblin.run();

    Assert.assertTrue(result.isSuccessful());

    ArrayList<String> expectedEvents = new ArrayList<>();
    for (int i = 1; i <= 10; ++i) {
      expectedEvents.add(HelloWorldSource.ExtractorImpl.helloMessage(i));
    }
    asserter.assertNextValuesEq(expectedEvents);
    asserter.close();
  }

  @Test
  public void testDistributedJars() throws Exception {
    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("Test");
    embeddedGobblin.distributeJarWithPriority("myJar", 0);
    embeddedGobblin.distributeJarWithPriority("myJar2", -100);
    embeddedGobblin.distributeJarWithPriority("myJar3", 10);
    List<String> jars = embeddedGobblin.getPrioritizedDistributedJars();

    Assert.assertEquals(jars.get(0), "myJar2");
    Assert.assertEquals(jars.get(jars.size() - 1), "myJar3");
  }

}
