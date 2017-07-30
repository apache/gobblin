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

package gobblin.util;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class PortUtilsTest {
  @Test
  public void testReplaceAbsolutePortToken() throws Exception {
    PortUtils.PortLocator portLocator = Mockito.mock(PortUtils.PortLocator.class);
    Mockito.when(portLocator.specific(1025)).thenReturn(1025);
    PortUtils portUtils = new PortUtils(portLocator);
    String actual = portUtils.replacePortTokens("-Dvar1=${PORT_1025}");
    Assert.assertEquals(actual, "-Dvar1=1025");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testFailIfCannotReplaceAbsolutePortToken() throws Exception {
    PortUtils.PortLocator portLocator = Mockito.mock(PortUtils.PortLocator.class);
    Mockito.when(portLocator.specific(1025)).thenThrow(new IOException());
    PortUtils portUtils = new PortUtils(portLocator);
    portUtils.replacePortTokens("-Dvar1=${PORT_1025}");
  }

  @Test
  public void testReplaceUnboundMinimumPortToken() throws Exception {
    int expectedPort = PortUtils.MINIMUM_PORT + 1;
    PortUtils.PortLocator portLocator = Mockito.mock(PortUtils.PortLocator.class);
    Mockito.when(portLocator.specific(PortUtils.MINIMUM_PORT)).thenThrow(new IOException());
    Mockito.when(portLocator.specific(expectedPort)).thenReturn(expectedPort);
    PortUtils portUtils = new PortUtils(portLocator);
    String actual = portUtils.replacePortTokens("-Dvar1=${PORT_?1026}");
    Assert.assertEquals(actual, "-Dvar1=1026");
  }

  @Test
  public void testReplaceUnboundMaximumPortToken() throws Exception {
    int expectedPort = PortUtils.MINIMUM_PORT + 1;
    PortUtils.PortLocator portLocator = Mockito.mock(PortUtils.PortLocator.class);
    Mockito.when(portLocator.specific(PortUtils.MINIMUM_PORT)).thenThrow(new IOException());
    Mockito.when(portLocator.specific(expectedPort)).thenReturn(expectedPort);
    PortUtils portUtils = new PortUtils(portLocator);
    String actual = portUtils.replacePortTokens("-Dvar1=${PORT_1025?}");
    Assert.assertEquals(actual, "-Dvar1=1026");
  }

  @Test
  public void testReplaceRandomPortToken() throws Exception {
    int expectedPort = PortUtils.MINIMUM_PORT + 1;
    PortUtils.PortLocator portLocator = Mockito.mock(PortUtils.PortLocator.class);
    Mockito.when(portLocator.random()).thenReturn(1027);
    PortUtils portUtils = new PortUtils(portLocator);
    String actual = portUtils.replacePortTokens("-Dvar1=${PORT_?}");
    Assert.assertEquals(actual, "-Dvar1=1027");
  }

  @Test
  public void testReplaceDuplicateTokensGetSamePort() throws Exception {
    final int expectedPort = PortUtils.MINIMUM_PORT + 1;
    PortUtils.PortLocator portLocator = Mockito.mock(PortUtils.PortLocator.class);
    Mockito.when(portLocator.random()).thenAnswer(new Answer<Integer>() {
      public int callCount;

      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        if (this.callCount++ == 0) {
          return expectedPort;
        }
        return expectedPort + callCount++;
      }
    });
    Mockito.when(portLocator.specific(expectedPort)).thenReturn(expectedPort);
    PortUtils portUtils = new PortUtils(portLocator);
    String actual = portUtils.replacePortTokens("-Dvar1=${PORT_?} -Dvar2=${PORT_?}");
    Assert.assertEquals(actual, "-Dvar1=1026 -Dvar2=1026");
  }

  @Test
  public void testReplacePortTokensKeepsTrackOfAssignedPorts() throws Exception {
    int expectedPort1 = PortUtils.MINIMUM_PORT + 1;
    int expectedPort2 = PortUtils.MINIMUM_PORT + 2;
    PortUtils.PortLocator portLocator = Mockito.mock(PortUtils.PortLocator.class);
    Mockito.when(portLocator.specific(PortUtils.MINIMUM_PORT)).thenThrow(new IOException());
    Mockito.when(portLocator.specific(expectedPort1)).thenReturn(expectedPort1);
    Mockito.when(portLocator.specific(expectedPort2)).thenReturn(expectedPort2);
    PortUtils portUtils = new PortUtils(portLocator);
    String actual = portUtils.replacePortTokens("-Dvar1=${PORT_1026?} -Dvar2=${PORT_1025?}");
    Assert.assertEquals(actual, "-Dvar1=1026 -Dvar2=1027");
  }
}
