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

package org.apache.gobblin.service.monitoring;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


/**
 * Unit tests for {@link JobStatusRetriever#getJobExecutionId(State)}.
 *
 * The method is {@code protected static}, so this test class lives in the same package to access it directly.
 */
public class JobStatusRetrieverGetJobExecutionIdTest {

  /** Returns a {@link State} with the given property value set for {@link ConfigurationKeys#GAAS_JOB_EXEC_ID_HASH}. */
  private static State stateWith(String value) {
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.GAAS_JOB_EXEC_ID_HASH, value);
    return new State(props);
  }

  @Test
  public void testValidId() {
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("12345")), 12345L);
  }

  @Test
  public void testPropertyNotSet_returnsZero() {
    // No property set — getProp defaults to "0", which parses to 0L
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(new State()), 0L);
  }

  @Test
  public void testExplicitZero_returnsZero() {
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("0")), 0L);
  }

  @Test
  public void testNonNumericString_returnsZero() {
    // NumberFormatException is caught internally; method falls back to 0
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("not-a-number")), 0L);
  }

  @Test
  public void testEmptyString_returnsZero() {
    // Empty string is skipped by the isEmpty() guard; returns default 0
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("")), 0L);
  }

  @Test
  public void testNegativeNumber_returnsParsedValue() {
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("-42")), -42L);
  }

  @Test
  public void testMaxLong_returnsParsedValue() {
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith(String.valueOf(Long.MAX_VALUE))), Long.MAX_VALUE);
  }

  @Test
  public void testLargePositiveId() {
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("9876543210")), 9876543210L);
  }

  @Test
  public void testHexString_returnsZero() {
    // Hex strings are not valid for Long.parseLong without radix
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("0xFF")), 0L);
  }

  @Test
  public void testFloatString_returnsZero() {
    // Floating point strings are not valid for Long.parseLong
    Assert.assertEquals(JobStatusRetriever.getJobExecutionId(stateWith("3.14")), 0L);
  }
}
