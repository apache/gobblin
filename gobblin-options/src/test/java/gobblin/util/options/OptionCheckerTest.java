/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.options;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;

import gobblin.util.options.classes.ClassWithCheckClasses;
import gobblin.util.options.classes.ClassWithCheckedFields;
import gobblin.util.options.classes.ClassWithReflectiveInstantiation;
import gobblin.util.options.classes.SimpleClass;
import gobblin.util.options.classes.SimpleClassExtended;
import gobblin.util.options.classes.Unchecked;

import static gobblin.util.options.OptionChecker.IssueType.*;

public class OptionCheckerTest {

  OptionChecker checker = new OptionChecker();

  private void reportContainsExactly(OptionChecker.Report report, List<OptionChecker.IssueType> issues) {
    for (OptionChecker.ReportEntry entry : report.getEntries()) {
      int idx = issues.indexOf(entry.getIssueType());
      Assert.assertTrue(idx >= 0, String.format("Issue not expected %s, issues expected %s.", entry.getIssueType(),
          Arrays.toString(issues.toArray())));
      issues.remove(idx);
    }
    Assert.assertTrue(issues.isEmpty(), "Issues not found " + Arrays.toString(issues.toArray()));
  }

  @Test
  public void testDirectOptions() throws IOException {
    Properties properties = new Properties();

    OptionChecker.Report report = checker.verify(SimpleClass.class, properties);
    reportContainsExactly(report, Lists.newArrayList(REQUIRED_BUT_MISSING));

    properties.setProperty(SimpleClass.OPTIONAL_OPT, "");
    report = checker.verify(SimpleClass.class, properties);
    reportContainsExactly(report, Lists.newArrayList(REQUIRED_BUT_MISSING));

    properties.setProperty(SimpleClass.REQUIRED_OPT, "");
    report = checker.verify(SimpleClass.class, properties);
    Assert.assertTrue(report.isSuccess());
  }

  @Test
  public void testCheckedFields() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ClassWithCheckedFields.REQUIRED_OPT, "");

    OptionChecker.Report report = checker.verify(ClassWithCheckedFields.class, properties);
    reportContainsExactly(report, Lists.newArrayList(REQUIRED_BUT_MISSING));

    properties.setProperty(SimpleClass.REQUIRED_OPT, "");
    report = checker.verify(ClassWithCheckedFields.class, properties);
    Assert.assertTrue(report.isSuccess());
  }

  @Test
  public void testClassWithReflectiveInstantiation() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ClassWithReflectiveInstantiation.INSTANTIATED_CLASS, "nonexistent.class");

    // Class doesn't exist
    properties.setProperty(ClassWithReflectiveInstantiation.INSTANTIATED_CLASS, "nonexistent.class");
    OptionChecker.Report report = checker.verify(ClassWithReflectiveInstantiation.class, properties);
    reportContainsExactly(report, Lists.newArrayList(CLASS_NOT_EXISTS));

    // wrong class type
    properties.setProperty(ClassWithReflectiveInstantiation.INSTANTIATED_CLASS,
        ClassWithCheckedFields.class.getCanonicalName());
    report = checker.verify(ClassWithReflectiveInstantiation.class, properties);
    reportContainsExactly(report, Lists.newArrayList(CLASS_WRONG_TYPE));

    // Correct type, but missing property of instantiated class
    properties.setProperty(ClassWithReflectiveInstantiation.INSTANTIATED_CLASS,
        SimpleClassExtended.class.getCanonicalName());
    report = checker.verify(ClassWithReflectiveInstantiation.class, properties);
    reportContainsExactly(report, Lists.newArrayList(REQUIRED_BUT_MISSING));

    properties.setProperty(SimpleClassExtended.REQUIRED_OPT, "");
    report = checker.verify(ClassWithReflectiveInstantiation.class, properties);
    Assert.assertTrue(report.isSuccess());

  }

  @Test
  public void testUnchecked() throws IOException {
    Properties properties = new Properties();
    OptionChecker.Report report = checker.verify(Unchecked.class, properties);
    reportContainsExactly(report, Lists.newArrayList(UNCHECKED));
  }

  @Test
  public void testCheckClasses() throws IOException {
    Properties properties = new Properties();
    OptionChecker.Report report = checker.verify(ClassWithCheckClasses.class, properties);
    reportContainsExactly(report, Lists.newArrayList(UNCHECKED, REQUIRED_BUT_MISSING));

    properties.setProperty(SimpleClass.REQUIRED_OPT, "");
    report = checker.verify(ClassWithCheckClasses.class, properties);
    reportContainsExactly(report, Lists.newArrayList(UNCHECKED));
  }

}
