/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.runtime;

import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NoSuchScopeException;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceKey;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.instance.StandardGobblinInstanceDriver;
import gobblin.runtime.instance.StandardGobblinInstanceLauncher;
import gobblin.runtime.std.DefaultJobLifecycleListenerImpl;
import gobblin.runtime.std.FilteredJobLifecycleListener;
import gobblin.runtime.std.JobSpecFilter;
import gobblin.writer.test.GobblinTestEventBusWriter;
import gobblin.writer.test.TestingEventBusAsserter;
import gobblin.writer.test.TestingEventBuses;
import gobblin.broker.ResourceInstance;

import lombok.Data;
import lombok.EqualsAndHashCode;


public class JobBrokerInjectionTest {

  @Test
  public void testBrokerIsAcquiredAndShared() throws Exception {
    StandardGobblinInstanceLauncher.Builder instanceLauncherBuilder =
        StandardGobblinInstanceLauncher.builder()
            .withInstanceName("testSubmitToJobCatalog");
    instanceLauncherBuilder.driver();
    StandardGobblinInstanceLauncher instanceLauncher =
        instanceLauncherBuilder.build();
    instanceLauncher.startAsync();
    instanceLauncher.awaitRunning(5, TimeUnit.SECONDS);

    JobSpec js1 = JobSpec.builder()
        .withConfig(ConfigFactory.parseResources("brokerTest/SimpleHelloWorldJob.jobconf"))
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
    JobExecutionResult jobResult = jobDriver.get(100000, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());

    Queue<TestingEventBuses.Event> events = asserter.getEvents();

    Set<Long> seenInstanceObjectIds = Sets.newHashSet();
    Set<Long> seenJobObjectIds = Sets.newHashSet();
    Set<Long> seenTaskObjectIds = Sets.newHashSet();

    for (TestingEventBuses.Event event : events) {
      MyRecord record = (MyRecord) event.getValue();
      seenInstanceObjectIds.add(record.getInstanceSharedObjectId());
      seenJobObjectIds.add(record.getJobSharedObjectId());
      seenTaskObjectIds.add(record.getTaskSharedObjectId());
    }

    // Should see same instance and job id (only 1 id in the set), but 5 different task ids for each task
    Assert.assertEquals(seenInstanceObjectIds.size(), 1);
    Assert.assertEquals(seenJobObjectIds.size(), 1);
    Assert.assertEquals(seenTaskObjectIds.size(), 5);


    asserter.clear();

    instance.getMutableJobCatalog().remove(js1.getUri());
    instance.getMutableJobCatalog().put(js1);

    jobDriver = jobDrivers.poll(10, TimeUnit.SECONDS);
    Assert.assertNotNull(jobDriver);
    jobResult = jobDriver.get(10, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());

    events = asserter.getEvents();

    for (TestingEventBuses.Event event : events) {
      MyRecord record = (MyRecord) event.getValue();
      seenInstanceObjectIds.add(record.getInstanceSharedObjectId());
      seenJobObjectIds.add(record.getJobSharedObjectId());
      seenTaskObjectIds.add(record.getTaskSharedObjectId());
    }

    // A different job should produce a new shared object id
    Assert.assertEquals(seenInstanceObjectIds.size(), 1);
    Assert.assertEquals(seenJobObjectIds.size(), 2);
    Assert.assertEquals(seenTaskObjectIds.size(), 10);
  }

  private void launchJob(StandardGobblinInstanceLauncher instanceLauncher, JobSpec js1,
      GobblinInstanceDriver instance) throws TimeoutException, InterruptedException, ExecutionException {
    JobExecutionDriver jobDriver = instance.getJobLauncher().launchJob(js1);
    new Thread(jobDriver).run();
    JobExecutionResult jobResult = jobDriver.get(5, TimeUnit.SECONDS);

    Assert.assertTrue(jobResult.isSuccessful());
  }

  public static class JobBrokerConverter extends Converter<String, String, String, MyRecord> {

    private MySharedObject instanceSharedObject;
    private MySharedObject jobSharedObject;
    private MySharedObject taskSharedObject;

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
      try {
        try {
          this.instanceSharedObject = workUnit.getTaskBroker()
              .getSharedResourceAtScope(new MySharedObjectFactory(), new MySharedKey(), GobblinScopeTypes.INSTANCE);
          this.jobSharedObject = workUnit.getTaskBroker()
              .getSharedResourceAtScope(new MySharedObjectFactory(), new MySharedKey(), GobblinScopeTypes.JOB);
          this.taskSharedObject = workUnit.getTaskBroker()
              .getSharedResourceAtScope(new MySharedObjectFactory(), new MySharedKey(), GobblinScopeTypes.TASK);

        } catch (NoSuchScopeException nsse) {
          throw new RuntimeException(nsse);
        }

        return inputSchema;
      } catch (NotConfiguredException nce) {
        throw new RuntimeException(nce);
      }
    }

    @Override
    public Iterable<MyRecord> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      return new SingleRecordIterable<>(new MyRecord(this.taskSharedObject.id, this.jobSharedObject.id, this.instanceSharedObject.id));
    }
  }

  @Data
  public static class MyRecord {
    private final long taskSharedObjectId;
    private final long jobSharedObjectId;
    private final long instanceSharedObjectId;
  }

  public static class MySharedObject {
    private final long id = new Random().nextLong();
  }

  public static class MySharedObjectFactory implements SharedResourceFactory<MySharedObject, MySharedKey, GobblinScopeTypes> {
    @Override
    public String getName() {
      return MySharedObjectFactory.class.getSimpleName();
    }

    @Override
    public ResourceInstance<MySharedObject> createResource(SharedResourcesBroker broker, ScopedConfigView<?, MySharedKey> config) {
      return new ResourceInstance<>(new MySharedObject());
    }

    @Override
    public GobblinScopeTypes getAutoScope(SharedResourcesBroker<GobblinScopeTypes> broker, ConfigView<GobblinScopeTypes, MySharedKey> config) {
      return broker.selfScope().getType();
    }
  }

  @EqualsAndHashCode
  public static class MySharedKey implements SharedResourceKey {
    @Override
    public String toConfigurationKey() {
      return "key";
    }
  }

}
