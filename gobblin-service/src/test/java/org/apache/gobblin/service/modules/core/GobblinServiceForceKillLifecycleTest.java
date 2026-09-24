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
package org.apache.gobblin.service.modules.core;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;

import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.ForceKillHandler;

import static org.mockito.Mockito.*;
import static org.powermock.reflect.Whitebox.setInternalState;


public class GobblinServiceForceKillLifecycleTest {
  @Test
  public void testManagerStopClosesBackendAndIsIdempotent() throws Exception {
    Fixture fixture = new Fixture();

    fixture.manager.stop();
    fixture.manager.stop();

    verify(fixture.launcher).stop();
    verify(fixture.handler).close();
  }

  @Test
  public void testBackendClosesWhenShutdownFailsBeforeLauncher() {
    Fixture fixture = new Fixture();
    IllegalStateException failure = new IllegalStateException("d2 failure");
    doThrow(failure).when(fixture.manager.d2Announcer).markDownServer();

    Assert.assertSame(Assert.expectThrows(IllegalStateException.class, fixture.manager::stop), failure);
    verify(fixture.handler).close();
    verifyNoInteractions(fixture.launcher);
  }

  @Test
  public void testBackendClosesWhenLauncherCloseFails() throws Exception {
    Fixture fixture = new Fixture();
    IOException failure = new IOException("launcher close failed");
    doThrow(failure).when(fixture.launcher).close();

    Assert.assertSame(Assert.expectThrows(IOException.class, fixture.manager::close), failure);
    verify(fixture.handler).close();
  }

  @Test
  public void testStartupFailureClosesBackendWithoutHidingOriginalFailure() {
    Fixture fixture = new Fixture();
    IllegalStateException failure = new IllegalStateException("configuration failed");
    when(fixture.configuration.isRestLIServerEnabled()).thenThrow(failure);
    IllegalStateException closeFailure = new IllegalStateException("close failed");
    doThrow(closeFailure).when(fixture.handler).close();

    Assert.assertSame(Assert.expectThrows(IllegalStateException.class, fixture.manager::start), failure);
    Assert.assertEquals(failure.getSuppressed(), new Throwable[] {closeFailure});
    verify(fixture.handler).close();
  }

  @Test
  public void testLauncherOwnShutdownPathAlsoClosesBackend() throws Exception {
    Fixture fixture = new Fixture();
    fixture.manager.flowCatalog = mock(FlowCatalog.class);
    IllegalStateException failure = new IllegalStateException("launcher start failed");
    doThrow(failure).when(fixture.launcher).start();
    Assert.assertSame(Assert.expectThrows(IllegalStateException.class, fixture.manager::start), failure);
    verify(fixture.handler).close();

    ArgumentCaptor<Service> registered = ArgumentCaptor.forClass(Service.class);
    verify(fixture.launcher, atLeastOnce()).addService(registered.capture());
    Service backendLifecycle = registered.getAllValues().stream().filter(Objects::nonNull)
        .filter(service -> service != fixture.manager.flowCatalog).findFirst().get();
    clearInvocations(fixture.handler);

    backendLifecycle.startAsync().awaitRunning(5, TimeUnit.SECONDS);
    backendLifecycle.stopAsync().awaitTerminated(5, TimeUnit.SECONDS);

    verify(fixture.handler).close();
  }

  @Test
  public void testNoBackendPreservesManagerShutdown() throws Exception {
    Fixture fixture = new Fixture();
    setInternalState(fixture.manager, "forceKillHandler", Optional.absent());

    fixture.manager.stop();
    fixture.manager.close();

    verify(fixture.launcher).stop();
    verify(fixture.launcher).close();
    verifyNoInteractions(fixture.handler);
  }

  @Test
  public void testSuccessfulStartupGuardDoesNotCloseBackend() {
    ForceKillHandler handler = mock(ForceKillHandler.class);
    try (GobblinServiceManager.ForceKillStartupGuard guard = new GobblinServiceManager.ForceKillStartupGuard(handler)) {
      guard.started = true;
    }
    verifyNoInteractions(handler);
  }

  private static final class Fixture {
    final ForceKillHandler handler = mock(ForceKillHandler.class);
    final ServiceBasedAppLauncher launcher = mock(ServiceBasedAppLauncher.class);
    final GobblinServiceConfiguration configuration = mock(GobblinServiceConfiguration.class);
    final GobblinServiceManager manager = mock(GobblinServiceManager.class, CALLS_REAL_METHODS);

    Fixture() {
      setInternalState(this.manager, "forceKillHandler", Optional.of(this.handler));
      setInternalState(this.manager, "serviceLauncher", this.launcher);
      setInternalState(this.manager, "configuration", this.configuration);
      this.manager.d2Announcer = mock(D2Announcer.class);
    }
  }
}
