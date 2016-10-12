/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.embedded;

import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.template.ResourceBasedJobTemplate;
import gobblin.util.test.HelloWorldSource;
import gobblin.writer.test.GobblinTestEventBusWriter;
import gobblin.writer.test.TestingEventBusAsserter;


public class EmbeddedGobblinTest {

  @Test
  public void test() throws Exception {
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

}
