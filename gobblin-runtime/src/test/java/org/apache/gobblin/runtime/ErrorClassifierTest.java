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

package org.apache.gobblin.runtime;

import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.configuration.ErrorPatternProfile;
import org.apache.gobblin.metastore.InMemoryErrorPatternStore;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueTestDataProvider;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;

import org.omg.CORBA.UNKNOWN;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static org.testng.Assert.*;


public class ErrorClassifierTest {
  private ErrorClassifier classifier;
  private List<ErrorCategory> categories;
  private ErrorCategory _defaultErrorCategory;
  private InMemoryErrorPatternStore store;

  @BeforeMethod
  public void setUp()
      throws IOException {
    // Use the shared test data
    categories = IssueTestDataProvider.TEST_CATEGORIES;
    _defaultErrorCategory = IssueTestDataProvider.TEST_DEFAULT_ERROR_CATEGORY;
    List<ErrorPatternProfile> errorPatternProfiles = IssueTestDataProvider.getSortedPatterns();

    Config testConfig = ConfigFactory.empty();
    store = new InMemoryErrorPatternStore(testConfig);
    store.upsertCategory(categories);
    store.upsertPatterns(errorPatternProfiles);
    store.setDefaultCategory(_defaultErrorCategory);

    classifier = new ErrorClassifier(store, testConfig);
  }

  @Test
  public void testUserCategoryIssues() {
    List<Issue> issues = IssueTestDataProvider.testUserCategoryIssues();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("USER"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
    assertTrue(
        result.getDetails().contains("permission denied") || result.getDetails().contains("FileNotFoundException"));
  }

  @Test
  public void testSystemInfraCategoryIssues() {
    List<Issue> issues = IssueTestDataProvider.testSystemInfraCategoryIssues();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("SYSTEM INFRA"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
    assertTrue(result.getDetails().contains("server too busy") || result.getDetails().contains("SafeModeException"));
  }

  @Test
  public void testGaasCategoryIssues() {
    List<Issue> issues = IssueTestDataProvider.testGaasCategoryIssues();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("GAAS"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
    assertTrue(result.getDetails().contains("TimeoutFailure") || result.getDetails().contains("ApplicationFailure"));
  }

  @Test
  public void testMiscCategoryIssues() {
    List<Issue> issues = IssueTestDataProvider.testMiscCategoryIssues();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("MISC"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
    assertTrue(
        result.getDetails().contains("NullPointerException") || result.getDetails().contains("InterruptedIOException"));
  }

  @Test
  public void testNonFatalCategoryIssues() {
    List<Issue> issues = IssueTestDataProvider.testNonFatalCategoryIssues();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("NON FATAL"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
    assertTrue(result.getDetails().contains("CopyDataPublisher") || result.getDetails()
        .contains("failed to delete temporary file"));
  }

  @Test
  public void testNegativeMatchCases() {
    List<Issue> issues = IssueTestDataProvider.testNegativeMatchCases();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("UNKNOWN"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testEdgeCasesAndBoundaryConditions() {
    List<Issue> issues = IssueTestDataProvider.testEdgeCasesAndBoundaryConditions();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("USER"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testNonFatalAndUnknownMix() {
    List<Issue> issues = IssueTestDataProvider.testNonFatalAndUnknownMix();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    // Assert based on category priority: if NON FATAL is higher, assert it; else assert UNKNOWN
    int nonFatalPriority = -1;
    int unknownPriority = -1;
    for (int i = 0; i < categories.size(); i++) {
      if (categories.get(i).getCategoryName().equals("NON FATAL")) {
        nonFatalPriority = categories.get(i).getPriority();
        assertEquals(nonFatalPriority, 6);

      }
      if (categories.get(i).getCategoryName().equals("UNKNOWN")) {
        unknownPriority = categories.get(i).getPriority();
        assertEquals(unknownPriority, 5);
      }
    }
    if (nonFatalPriority != -1 && (unknownPriority == -1 || nonFatalPriority < unknownPriority)) {
      assertTrue(result.getSummary().contains("NON FATAL"));
    } else {
      assertTrue(result.getSummary().contains("UNKNOWN"));
    }
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testMaximumErrorCountExceeded() {
    List<Issue> issues = IssueTestDataProvider.testMaximumErrorCountExceeded();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("USER"));

    assertTrue(result.getDetails().contains("/path/to/missing/file1"));
    assertTrue(result.getDetails().contains("/path/to/missing/file9"));

    assertFalse(result.getDetails().contains("/path/to/missing/file11"));
    assertFalse(result.getDetails().contains("/path/to/missing/file14"));

    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testMinimumErrorCountBelowThreshold() {
    List<Issue> issues = IssueTestDataProvider.testMinimumErrorCountBelowThreshold();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);

    assertTrue(result.getSummary().contains("USER"));
    assertTrue(result.getDetails().contains("FileNotFoundException"));

    assertFalse(result.getDetails().contains("server too busy"));

    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testMixedCategoryIssuesFromLowestToHighestPriority() {
    List<Issue> issues = IssueTestDataProvider.testMixedCategoryIssuesFromLowestToHighestPriority();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("USER"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }
}



