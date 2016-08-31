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
package gobblin.runtime.instance;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;

import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.api.JobSpec;

/**
 * Unit tests for {@link StandardGobblinInstanceLauncher}
 */
public class TestStandardGobblinInstanceLauncher {

  @Test
  public void testSimpleScenario() throws Exception {
    StandardGobblinInstanceLauncher.Builder instanceLauncherBuilder =
        StandardGobblinInstanceLauncher.builder()
        .withInstanceName("testSimpleScenario");
    instanceLauncherBuilder.driver();
    StandardGobblinInstanceLauncher instanceLauncher =
        instanceLauncherBuilder.build();
    instanceLauncher.startAsync();
    instanceLauncher.awaitRunning(50, TimeUnit.MILLISECONDS);

    JobSpec js1 = JobSpec.builder()
        .withConfig(ConfigFactory.parseResources("gobblin/runtime/instance/SimpleHelloWorldJob.jobconf"))
        .build();
    GobblinInstanceDriver instance = instanceLauncher.getDriver();
    JobExecutionDriver jobDriver = instance.getJobLauncher().launchJob(js1);
    jobDriver.startAsync();
    JobExecutionResult jobResult = jobDriver.get(5, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());

    instanceLauncher.stopAsync();
    instanceLauncher.awaitTerminated(50, TimeUnit.MILLISECONDS);
  }

}
