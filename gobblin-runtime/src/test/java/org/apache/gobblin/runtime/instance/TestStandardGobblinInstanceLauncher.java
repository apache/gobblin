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
package org.apache.gobblin.runtime.instance;

import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.JobExecutionDriver;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.api.JobLifecycleListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.instance.DefaultGobblinInstanceDriverImpl.JobSpecRunnable;
import org.apache.gobblin.runtime.job_spec.ResolvedJobSpec;
import org.apache.gobblin.runtime.std.DefaultJobLifecycleListenerImpl;
import org.apache.gobblin.runtime.std.FilteredJobLifecycleListener;
import org.apache.gobblin.runtime.std.JobSpecFilter;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.test.HelloWorldSource;
import org.apache.gobblin.writer.test.GobblinTestEventBusWriter;
import org.apache.gobblin.writer.test.TestingEventBusAsserter;

/**
 * Unit tests for {@link StandardGobblinInstanceLauncher}
 */
public class TestStandardGobblinInstanceLauncher {

  @Test
  /** Test running of a job when submitted directly to the execution driver*/
  public void testDirectToExecutionDriver() throws Exception {
    StandardGobblinInstanceLauncher.Builder instanceLauncherBuilder =
        StandardGobblinInstanceLauncher.builder()
        .withInstanceName("testDirectToExecutionDriver");
    instanceLauncherBuilder.driver();
    StandardGobblinInstanceLauncher instanceLauncher =
        instanceLauncherBuilder.build();
    instanceLauncher.startAsync();
    instanceLauncher.awaitRunning(5, TimeUnit.SECONDS);

    JobSpec js1 = JobSpec.builder()
        .withConfig(ConfigFactory.parseResources("gobblin/runtime/instance/SimpleHelloWorldJob.jobconf"))
        .build();
    GobblinInstanceDriver instance = instanceLauncher.getDriver();
    final JobExecutionLauncher.StandardMetrics launcherMetrics =
        instance.getJobLauncher().getMetrics();

    AssertWithBackoff asb = new AssertWithBackoff().timeoutMs(100);

    checkLaunchJob(instanceLauncher, js1, instance);
    Assert.assertEquals(launcherMetrics.getNumJobsLaunched().getCount(), 1);
    Assert.assertEquals(launcherMetrics.getNumJobsCompleted().getCount(), 1);
    // Need to use assert with backoff because of race conditions with the callback that updates the
    // metrics
    asb.assertEquals(new Function<Void, Long>() {
      @Override public Long apply(Void input) {
        return launcherMetrics.getNumJobsCommitted().getCount();
      }
    }, 1l, "numJobsCommitted==1");
    Assert.assertEquals(launcherMetrics.getNumJobsFailed().getCount(), 0);
    Assert.assertEquals(launcherMetrics.getNumJobsRunning().getValue().intValue(), 0);

    checkLaunchJob(instanceLauncher, js1, instance);
    Assert.assertEquals(launcherMetrics.getNumJobsLaunched().getCount(), 2);
    Assert.assertEquals(launcherMetrics.getNumJobsCompleted().getCount(), 2);
    asb.assertEquals(new Function<Void, Long>() {
      @Override public Long apply(Void input) {
        return launcherMetrics.getNumJobsCommitted().getCount();
      }
    }, 2l, "numJobsCommitted==2");
    Assert.assertEquals(launcherMetrics.getNumJobsFailed().getCount(), 0);
    Assert.assertEquals(launcherMetrics.getNumJobsRunning().getValue().intValue(), 0);
  }


  private void checkLaunchJob(StandardGobblinInstanceLauncher instanceLauncher, JobSpec js1,
      GobblinInstanceDriver instance) throws TimeoutException, InterruptedException, ExecutionException {
    JobExecutionDriver jobDriver = instance.getJobLauncher().launchJob(js1);
    new Thread(jobDriver).run();
    JobExecutionResult jobResult = jobDriver.get(5, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());

    instanceLauncher.stopAsync();
    instanceLauncher.awaitTerminated(5, TimeUnit.SECONDS);
    Assert.assertEquals(instance.getMetrics().getUpFlag().getValue().intValue(), 0);
    Assert.assertEquals(instance.getMetrics().getUptimeMs().getValue().longValue(), 0);
  }


  @Test
  /** Test running of a job when submitted directly to the scheduler */
  public void testDirectToScheduler() throws Exception {
    StandardGobblinInstanceLauncher.Builder instanceLauncherBuilder =
        StandardGobblinInstanceLauncher.builder()
        .withInstanceName("testDirectToScheduler");
    instanceLauncherBuilder.driver();
    StandardGobblinInstanceLauncher instanceLauncher =
        instanceLauncherBuilder.build();
    instanceLauncher.startAsync();
    instanceLauncher.awaitRunning(5, TimeUnit.SECONDS);

    JobSpec js1 = JobSpec.builder()
        .withConfig(ConfigFactory.parseResources("gobblin/runtime/instance/SimpleHelloWorldJob.jobconf"))
        .build();
    final StandardGobblinInstanceDriver instance =
        (StandardGobblinInstanceDriver)instanceLauncher.getDriver();

    final ArrayBlockingQueue<JobExecutionDriver> jobDrivers = new ArrayBlockingQueue<>(1);

    JobLifecycleListener js1Listener = new FilteredJobLifecycleListener(
        JobSpecFilter.eqJobSpecURI(js1.getUri()),
        new DefaultJobLifecycleListenerImpl(instance.getLog()) {
            @Override public void onJobLaunch(JobExecutionDriver jobDriver) {
              super.onJobLaunch(jobDriver);
              try {
                jobDrivers.offer(jobDriver, 5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                instance.getLog().error("Offer interrupted.");
              }
            }
          });
    instance.registerWeakJobLifecycleListener(js1Listener);

    JobSpecRunnable js1Runnable = instance.createJobSpecRunnable(js1);
    instance.getJobScheduler().scheduleOnce(js1, js1Runnable);

    JobExecutionDriver jobDriver = jobDrivers.poll(10, TimeUnit.SECONDS);
    Assert.assertNotNull(jobDriver);
    JobExecutionResult jobResult = jobDriver.get(5, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());

    instanceLauncher.stopAsync();
    instanceLauncher.awaitTerminated(5, TimeUnit.SECONDS);
  }


  @Test
  /** Test running of a job using the standard path of submitting to the job catalog */
  public void testSubmitToJobCatalog() throws Exception {
    StandardGobblinInstanceLauncher.Builder instanceLauncherBuilder =
        StandardGobblinInstanceLauncher.builder()
        .withInstanceName("testSubmitToJobCatalog");
    instanceLauncherBuilder.driver();
    StandardGobblinInstanceLauncher instanceLauncher =
        instanceLauncherBuilder.build();
    instanceLauncher.startAsync();
    instanceLauncher.awaitRunning(5, TimeUnit.SECONDS);

    JobSpec js1 = JobSpec.builder()
        .withConfig(ConfigFactory.parseResources("gobblin/runtime/instance/SimpleHelloWorldJob.jobconf"))
        .build();

    final String eventBusId = js1.getConfig().resolve().getString(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY);
    TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId);

    final StandardGobblinInstanceDriver instance =
        (StandardGobblinInstanceDriver)instanceLauncher.getDriver();

    final ArrayBlockingQueue<JobExecutionDriver> jobDrivers = new ArrayBlockingQueue<>(1);

    JobLifecycleListener js1Listener = new FilteredJobLifecycleListener(
        JobSpecFilter.eqJobSpecURI(js1.getUri()),
        new DefaultJobLifecycleListenerImpl(instance.getLog()) {
            @Override public void onJobLaunch(JobExecutionDriver jobDriver) {
              super.onJobLaunch(jobDriver);
              try {
                jobDrivers.offer(jobDriver, 5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                instance.getLog().error("Offer interrupted.");
              }
            }
          });
    instance.registerWeakJobLifecycleListener(js1Listener);

    instance.getMutableJobCatalog().put(js1);

    JobExecutionDriver jobDriver = jobDrivers.poll(10, TimeUnit.SECONDS);
    Assert.assertNotNull(jobDriver);
    JobExecutionResult jobResult = jobDriver.get(5, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());
    instanceLauncher.stopAsync();

    final int numHellos = js1.getConfig().getInt(HelloWorldSource.NUM_HELLOS_FULL_KEY);
    ArrayList<String> expectedEvents = new ArrayList<>();
    for (int i = 1; i <= numHellos; ++i) {
      expectedEvents.add(HelloWorldSource.ExtractorImpl.helloMessage(i));
    }
    asserter.assertNextValuesEq(expectedEvents);
    asserter.close();

    instanceLauncher.awaitTerminated(5, TimeUnit.SECONDS);
  }

  @Test
  public void testSubmitWithTemplate() throws Exception {
    StandardGobblinInstanceLauncher.Builder instanceLauncherBuilder =
        StandardGobblinInstanceLauncher.builder()
            .withInstanceName("testSubmitWithTemplate");
    instanceLauncherBuilder.driver();
    StandardGobblinInstanceLauncher instanceLauncher =
        instanceLauncherBuilder.build();
    instanceLauncher.startAsync();
    instanceLauncher.awaitRunning(5, TimeUnit.SECONDS);

    JobSpec js1 = JobSpec.builder()
        .withConfig(ConfigFactory.parseMap(ImmutableMap.of("numHellos", "5")))
        .withTemplate(new URI("resource:///gobblin/runtime/instance/SimpleHelloWorldJob.template"))
        .build();

    ResolvedJobSpec js1Resolved = new ResolvedJobSpec(js1);
    final String eventBusId = js1Resolved.getConfig().getString(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY);
    TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId);

    final StandardGobblinInstanceDriver instance =
        (StandardGobblinInstanceDriver)instanceLauncher.getDriver();

    final ArrayBlockingQueue<JobExecutionDriver> jobDrivers = new ArrayBlockingQueue<>(1);

    JobLifecycleListener js1Listener = new FilteredJobLifecycleListener(
        JobSpecFilter.eqJobSpecURI(js1.getUri()),
        new DefaultJobLifecycleListenerImpl(instance.getLog()) {
          @Override public void onJobLaunch(JobExecutionDriver jobDriver) {
            super.onJobLaunch(jobDriver);
            try {
              jobDrivers.offer(jobDriver, 5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              instance.getLog().error("Offer interrupted.");
            }
          }
        });
    instance.registerWeakJobLifecycleListener(js1Listener);

    instance.getMutableJobCatalog().put(js1);

    JobExecutionDriver jobDriver = jobDrivers.poll(10, TimeUnit.SECONDS);
    Assert.assertNotNull(jobDriver);
    JobExecutionResult jobResult = jobDriver.get(5, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());
    instanceLauncher.stopAsync();

    final int numHellos = js1Resolved.getConfig().getInt(HelloWorldSource.NUM_HELLOS_FULL_KEY);
    ArrayList<String> expectedEvents = new ArrayList<>();
    for (int i = 1; i <= numHellos; ++i) {
      expectedEvents.add(HelloWorldSource.ExtractorImpl.helloMessage(i));
    }
    asserter.assertNextValuesEq(expectedEvents);
    asserter.close();

    instanceLauncher.awaitTerminated(5, TimeUnit.SECONDS);
  }


}
