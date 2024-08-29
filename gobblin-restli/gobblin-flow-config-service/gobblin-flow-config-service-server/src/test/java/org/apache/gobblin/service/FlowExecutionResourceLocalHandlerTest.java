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

package org.apache.gobblin.service;


import org.testng.Assert;
import org.testng.annotations.Test;


public class FlowExecutionResourceLocalHandlerTest {

  @Test
  public void testEstimateCopyTimeLeftSanityCheck() throws Exception {
    long currentTime = 10000;
    long startTime = 0;
    int copyPercentage = 50;

    long timeLeft = FlowExecutionResourceLocalHandler.estimateCopyTimeLeft(currentTime, startTime, copyPercentage);
    Assert.assertEquals(timeLeft, 10);
  }

  @Test
  public void testEstimateCopyTimeLeftSimple() throws Exception {
    long currentTime = 50000;
    long startTime = 20000;
    int copyPercentage = 10;

    long timeLeft = FlowExecutionResourceLocalHandler.estimateCopyTimeLeft(currentTime, startTime, copyPercentage);
    Assert.assertEquals(timeLeft, 270);
  }

  @Test
  public void testEstimateCopyTimeLeftMedium() throws Exception {
    long currentTime = 5000000;
    long startTime = 1500000;
    int copyPercentage = 25;

    long timeLeft = FlowExecutionResourceLocalHandler.estimateCopyTimeLeft(currentTime, startTime, copyPercentage);
    Assert.assertEquals(timeLeft, 10500);
  }

  @Test
  public void testEstimateCopyTimeRealData() throws Exception {
    long currentTime = 1626717751099L;
    long startTime = 1626716510626L;
    int copyPercentage = 24;

    long timeLeft = FlowExecutionResourceLocalHandler.estimateCopyTimeLeft(currentTime, startTime, copyPercentage);
    Assert.assertEquals(timeLeft, 3926L);
  }
}