package org.apache.gobblin.runtime;

import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.configuration.ErrorPatternProfile;
import org.apache.gobblin.metastore.InMemoryErrorPatternStore;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueTestDataProvider;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;

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
      }
      if (categories.get(i).getCategoryName().equals("UNKNOWN")) {
        unknownPriority = categories.get(i).getPriority();
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
    // Threshold count
    assertTrue(result.getSummary().contains("USER"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testMinimumErrorCountBelowThreshold() {
    List<Issue> issues = IssueTestDataProvider.testMinimumErrorCountBelowThreshold();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("USER"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testOverflowLargeNumberOfErrors() {
    List<Issue> issues = IssueTestDataProvider.testOverflowLargeNumberOfErrors();
    Issue result = classifier.classifyEarlyStopWithDefault(issues);
    assertNotNull(result);
    assertTrue(result.getSummary().contains("USER"));
    assertEquals(result.getSeverity(), IssueSeverity.ERROR);
  }
}



