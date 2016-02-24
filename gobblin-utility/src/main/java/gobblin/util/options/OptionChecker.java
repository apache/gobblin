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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Strings;

import gobblin.util.options.annotations.Checked;


/**
 * Checks {@link Properties} against the user properties of a class to determine whether the {@link Properties} object
 * is valid.
 */
public class OptionChecker {

  private OptionFinder finder = new OptionFinder();

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.println(String.format("Usage: java -cp <classpath> %s <class> <properties-file>",
          OptionChecker.class.getCanonicalName()));
      System.exit(1);
    }

    Class<?> classToCheck = Class.forName(args[0]);
    Properties properties = new Properties();
    properties.load(new FileInputStream(new File(args[1])));

    System.out.println(new OptionChecker().verify(classToCheck, properties));
  }

  /**
   * Verify that the input properties objects is a valid configuration for input class.
   * @param klazz Class to check against. Class should be annotated with {@link Checked}.
   * @param properties user {@link Properties} to check.
   * @return true if configuration is valid.
   * @throws IOException
   */
  public Report verify(Class<?> klazz, Properties properties) throws IOException {
    Report.ReportBuilder reportBuilder = Report.builder();
    verify(klazz, properties, reportBuilder);
    return reportBuilder.build();
  }

  private void verify(Class<?> klazz, Properties properties, Report.ReportBuilder report)
      throws IOException {

    Map<Class<?>, List<UserOption>> userOptions = this.finder.findOptionsForClass(klazz);

    for (Class<?> aClass : userOptions.keySet()) {
      if (!aClass.isAnnotationPresent(Checked.class)) {
        report.entry(new ReportEntry(aClass, null, IssueType.UNCHECKED, ""));
      }
    }

    for (List<UserOption> optionList : userOptions.values()) {
      for (UserOption option : optionList) {
        if (properties.containsKey(option.getKey())) {
          Class<?> requiredType;
          if ((requiredType = option.getInstantiatesClass()) != null) {
            try {
              Class<?> instantiated = Class.forName(properties.getProperty(option.getKey()));
              if (!requiredType.isAssignableFrom(instantiated)) {
                report.entry(new ReportEntry(klazz, option, IssueType.CLASS_WRONG_TYPE, String
                    .format("Expected: %s, found: %s", instantiated.getCanonicalName(), requiredType.getCanonicalName())));
              } else {
                verify(instantiated, properties, report);
              }
            } catch (ClassNotFoundException cnfe) {
              report.entry(new ReportEntry(klazz, option, IssueType.CLASS_NOT_EXISTS, properties.getProperty(option.getKey())));
            }
          }
        } else if (option.isRequired()) {
          report.entry(new ReportEntry(klazz, option, IssueType.REQUIRED_BUT_MISSING, option.getKey()));
        }
        if (option.getValues() != null && properties.containsKey(option.getKey())) {
          String value = properties.getProperty(option.getKey());
          if (!option.getValueStrings().contains(value.toUpperCase())) {
            report.entry(new ReportEntry(klazz, option, IssueType.NOT_ACCEPTABLE_VALUE, value));
          }
        }
      }
    }

  }

  /**
   * Contains the result of a configuration validation.
   */
  @Getter
  public static class Report {

    private final List<ReportEntry> entries;
    private final boolean success;

    @Builder
    private Report(@Singular List<ReportEntry> entries) {
      this.entries = entries;
      boolean successTmp = true;
      for (ReportEntry entry : this.entries) {
        successTmp &= !entry.getIssueType().severe;
      }
      this.success = successTmp;
    }

    @Override public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(this.success ? "PASS" : "FAIL");
      builder.append("\n\n");
      builder.append("Issues\n");
      builder.append("----------------------------\n");
      for (ReportEntry entry : this.entries) {
        builder.append(entry.toString());
        builder.append("\n");
      }
      builder.append("\n");
      return builder.toString();
    }
  }

  /**
   * An entry for a {@link Report}.
   */
  @AllArgsConstructor
  @Getter
  public static class ReportEntry {
    private final Class<?> klazz;
    private final UserOption option;
    private final IssueType issueType;
    private final String comment;

    @Override public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(this.issueType.severe ? "ERROR: " : "WARN: ");
      if (this.option != null) {
        builder.append(this.option.getKey() + " at ");
      }
      builder.append(this.klazz.getCanonicalName());
      builder.append(" -> ");
      builder.append(this.issueType.message);
      if (!Strings.isNullOrEmpty(this.comment)) {
        builder.append(": ");
        builder.append(this.comment);
      }

      return builder.toString();
    }
  }

  @AllArgsConstructor
  public static enum IssueType {
    REQUIRED_BUT_MISSING(true, "Required property missing"),
    CLASS_NOT_EXISTS(true, "Class does not exist"),
    CLASS_WRONG_TYPE(true, "Class is not of expected type"),
    UNCHECKED(false, "Class is not validatable"),
    NOT_ACCEPTABLE_VALUE(false, "Value is not allowed");

    private final boolean severe;
    private final String message;
  }

}
