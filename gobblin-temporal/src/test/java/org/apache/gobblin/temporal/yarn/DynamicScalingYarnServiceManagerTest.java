package org.apache.gobblin.temporal.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;
import org.apache.gobblin.temporal.loadgen.dynamic.DummyScalingDirectiveSource;

/** Tests for {@link AbstractDynamicScalingYarnServiceManager.GetScalingDirectivesRunnable}*/
public class DynamicScalingYarnServiceManagerTest {

  private DynamicScalingYarnService mockDynamicScalingYarnService;
  private ScalingDirectiveSource mockScalingDirectiveSource;

  @BeforeMethod
  public void setup() {
    mockDynamicScalingYarnService = Mockito.mock(DynamicScalingYarnService.class);
    mockScalingDirectiveSource = Mockito.mock(ScalingDirectiveSource.class);
  }

  @Test
  public void testWhenScalingDirectivesIsNull() throws IOException {
    Mockito.when(mockScalingDirectiveSource.getScalingDirectives()).thenReturn(null);
    Runnable runnable = new AbstractDynamicScalingYarnServiceManager.GetScalingDirectivesRunnable(
        mockDynamicScalingYarnService, mockScalingDirectiveSource);
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(0)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
  }

  @Test
  public void testWhenScalingDirectivesIsEmpty() throws IOException {
    Mockito.when(mockScalingDirectiveSource.getScalingDirectives()).thenReturn(new ArrayList<>());
    Runnable runnable = new AbstractDynamicScalingYarnServiceManager.GetScalingDirectivesRunnable(
        mockDynamicScalingYarnService, mockScalingDirectiveSource);
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(0)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
  }

  /** Note : this test uses {@link org.apache.gobblin.temporal.loadgen.dynamic.DummyScalingDirectiveSource}*/
  @Test
  public void testWithDummyScalingDirectiveSource() {
    // DummyScalingDirectiveSource returns 2 scaling directives in first 3 invocations and after that it returns empty list
    // so the total number of invocations after three invocations should always be 3
    Runnable runnable = new AbstractDynamicScalingYarnServiceManager.GetScalingDirectivesRunnable(
        mockDynamicScalingYarnService, new DummyScalingDirectiveSource());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(1)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(2)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(3)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(3)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(3)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
  }

  @Test
  public void testWithRandomScalingDirectives() throws IOException {
    ScalingDirective mockScalingDirective = Mockito.mock(ScalingDirective.class);
    List<ScalingDirective> mockedScalingDirectives = Arrays.asList(mockScalingDirective, mockScalingDirective);
    Mockito.when(mockScalingDirectiveSource.getScalingDirectives())
        .thenReturn(new ArrayList<>())
        .thenReturn(mockedScalingDirectives)
        .thenReturn(null)
        .thenReturn(mockedScalingDirectives);

    Runnable runnable = new AbstractDynamicScalingYarnServiceManager.GetScalingDirectivesRunnable(
        mockDynamicScalingYarnService, mockScalingDirectiveSource);

    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(0)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(1)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(1)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    runnable.run();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(2)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
  }


}
