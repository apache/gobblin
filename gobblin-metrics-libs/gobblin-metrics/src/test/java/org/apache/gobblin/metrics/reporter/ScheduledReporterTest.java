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

package gobblin.metrics.reporter;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import gobblin.metrics.MetricContext;
import gobblin.metrics.context.ReportableContext;
import gobblin.metrics.context.filter.ContextFilterFactory;
import gobblin.metrics.test.ContextStoreReporter;
import gobblin.util.ConfigUtils;


/**
 * Test for {@link gobblin.metrics.reporter.ScheduledReporter}
 */
public class ScheduledReporterTest {

  @Test
  public void testPeriodParser() {

    Assert.assertEquals(ScheduledReporter.parsePeriodToSeconds("1s"), 1);
    Assert.assertEquals(ScheduledReporter.parsePeriodToSeconds("2m"), 120);
    Assert.assertEquals(ScheduledReporter.parsePeriodToSeconds("3h"), 3 * 3600);
    Assert.assertEquals(ScheduledReporter.parsePeriodToSeconds("1m2s"), 62);
    Assert.assertEquals(ScheduledReporter.parsePeriodToSeconds("1h1s"), 3601);
    Assert.assertEquals(ScheduledReporter.parsePeriodToSeconds("1h2m3s"), 3600 + 120 + 3);

    try {
      ScheduledReporter.parsePeriodToSeconds("1000000h");
      Assert.fail();
    } catch (RuntimeException re) {
      // fail unless exception is thrown
    }
  }

  @Test
  public void testScheduledReporter() throws Exception {

    long reportingIntervalMillis = 1000;

    String context1Name = ScheduledReporterTest.class.getSimpleName() + "_1";
    String context2Name = ScheduledReporterTest.class.getSimpleName() + "_2";
    String context3Name = "SomeOtherName";

    // Create a context name (to check that initialized reporter gets existing contexts correctly)
    MetricContext context1 = MetricContext.builder(context1Name).build();

    // Set up config for reporter
    Properties props = new Properties();
    ScheduledReporter.setReportingInterval(props, reportingIntervalMillis, TimeUnit.MILLISECONDS);
    Config config = ConfigUtils.propertiesToConfig(props);
    config = PrefixContextFilter.setPrefixString(config, ScheduledReporterTest.class.getSimpleName());
    config = ContextFilterFactory.setContextFilterClass(config, PrefixContextFilter.class);

    // Create reporter
    ContextStoreReporter reporter = new ContextStoreReporter("testContext", config);

    // Check that reporter correctly found created context
    Set<String> contextNames = getContextNames(reporter);
    Assert.assertEquals(contextNames.size(), 1);
    Assert.assertTrue(contextNames.contains(context1Name));

    // Create two more contexts
    MetricContext context2 = context1.childBuilder(context2Name).build();
    context1.childBuilder(context3Name).build();

    // Check that reporter correctly found new reporter, but skipped the one that does not satisfy filter
    contextNames = getContextNames(reporter);
    Assert.assertEquals(contextNames.size(), 2);
    Assert.assertTrue(contextNames.contains(context1Name));
    Assert.assertTrue(contextNames.contains(context2Name));

    // Check that nothing has been reported
    Assert.assertEquals(reporter.getReportedContexts().size(), 0);

    // Start reporter
    reporter.start();

    // Wait for up to 10 reporting intervals for 3 reports to run
    long maxWaitMillis = 10 * reportingIntervalMillis;
    long totalWait = 0;
    while(reporter.getReportedContexts().size() < 6 && maxWaitMillis > 0) {
      long wait = 100;
      Thread.sleep(wait);
      maxWaitMillis -= wait;
      totalWait += wait;
    }

    // stop reporter
    reporter.stop();

    // Check wait makes sense given reporting interval (e.g. if wait = 100 millis, then 2 reports in 100 millis,
    // something is wrong with schedule).
    Assert.assertTrue(totalWait > reportingIntervalMillis);
    Assert.assertTrue(reporter.getReportedContexts().size() >= 6);
    // Check that it didn't report excessively
    Assert.assertTrue(reporter.getReportedContexts().size() <= 10);

    // Check that first report indeed reported the correct contexts
    Set<String> firstReport = Sets.newHashSet(reporter.getReportedContexts().get(0).getName(),
        reporter.getReportedContexts().get(1).getName());
    Assert.assertEquals(firstReport, Sets.newHashSet(context1Name, context2Name));

    // Check that second report indeed reported the correct contexts
    Set<String> secondReport = Sets.newHashSet(reporter.getReportedContexts().get(2).getName(),
        reporter.getReportedContexts().get(3).getName());
    Assert.assertEquals(secondReport, Sets.newHashSet(context1Name, context2Name));

    int totalReports = reporter.getReportedContexts().size();

    // Wait for reporting interval to make sure reporting has actually stopped
    Thread.sleep(2 * reportingIntervalMillis);
    Assert.assertEquals(reporter.getReportedContexts().size(), totalReports);
    reporter.getReportedContexts().clear();

    // Dereference context 2 to ensure that it gets reported
    context2 = null;

    // Wait for context to be GCed
    maxWaitMillis = 2000;
    System.gc();
    while(reporter.getReportedContexts().size() < 1 && maxWaitMillis > 0) {
      System.gc();
      long wait = 100;
      Thread.sleep(wait);
      maxWaitMillis -= wait;
    }

    // Check that GCed context was reported
    Assert.assertEquals(reporter.getReportedContexts().size(), 1);
    Assert.assertEquals(reporter.getReportedContexts().get(0).getName(), context2Name);

    // Test close method
    reporter.close();
  }

  private Set<String> getContextNames(ContextStoreReporter reporter) {
    return Sets.newHashSet(Iterables.transform(reporter.getContextsToReport(),
        new Function<ReportableContext, String>() {
          @Nullable @Override public String apply(ReportableContext input) {
            return input.getName();
          }
        }));
  }
}
