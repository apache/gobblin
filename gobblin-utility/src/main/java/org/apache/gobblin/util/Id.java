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

package org.apache.gobblin.util;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Getter;


public abstract class Id {
  public static final String SEPARATOR = "_";
  private static final Joiner JOINER = Joiner.on(SEPARATOR).skipNulls();
  static final Pattern PATTERN = Pattern.compile("([^" + SEPARATOR + "]+)" + SEPARATOR + "(.+)" + SEPARATOR + "(\\d+)");

  public enum Parts {
    PREFIX,
    NAME,
    SEQUENCE;

    public static final EnumSet<Parts> INSTANCE_NAME = EnumSet.of(NAME, SEQUENCE);
    public static final EnumSet<Parts> ALL = EnumSet.allOf(Parts.class);
  }

  public Id(String name) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name is null or empty.");

      this.name = name;
  }

  public Id(String name, long sequence) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name is null or empty.");

      this.name = name;
      this.sequence = sequence;
  }

  protected abstract String getPrefix();

  @Getter
  private String name;

  @Getter
  private Long sequence;

  public String get(EnumSet<Parts> parts) {
    Object[] values = new Object[3];
    if (parts.contains(Parts.PREFIX)) {
      values[0] = getPrefix();
    }
    if (parts.contains(Parts.NAME)) {
      values[1] = name;
    }
    if (parts.contains(Parts.SEQUENCE)) {
      values[2] = sequence;
    }
    return JOINER.join(values);
  }

  @Override
  public String toString() {
    return get(Parts.ALL);
  }

  public static Id parse(String id) {
    Matcher matcher = PATTERN.matcher(id);
    if (matcher.find()) {
      if (Job.PREFIX.equals(matcher.group(1))) {
        return new Job(matcher.group(2), matcher.group(3));
      }
      if (Task.PREFIX.equals(matcher.group(1))) {
        return new Task(matcher.group(2), matcher.group(3));
      }
      if (MultiTask.PREFIX.equals(matcher.group(1))) {
        return new MultiTask(matcher.group(2), matcher.group(3));
      }
    }
    throw new RuntimeException("Invalid id: " + id);
  }

  public static class Job extends Id {
    public static final String PREFIX = "job";

    private Job(String name) {
      super(name);
    }

    private Job(String name, long sequence) {
      super(name, sequence);
    }

    private Job(String name, String sequence) {
      super(name, Long.parseLong(sequence));
    }

    @Override
    protected String getPrefix() {
      return PREFIX;
    }

    public static Job create(String name) {
      return new Job(name);
    }

    public static Job create(String name, long sequence) {
      return new Job(name, sequence);
    }
  }

  public static class Task extends Id {
    public static final String PREFIX = "task";

    private Task(String name) {
      super(name);
    }

    private Task(String name, int sequence) {
      super(name, sequence);
    }

    private Task(String name, String sequence) {
      super(name, Integer.parseInt(sequence));
    }

    @Override
    protected String getPrefix() {
      return PREFIX;
    }

    public static Task create(String name) {
      return new Task(name);
    }

    public static Task create(String name, int sequence) {
      return new Task(name, sequence);
    }
  }

  public static class MultiTask extends Id {
    public static final String PREFIX = "multitask";

    private MultiTask(String name) {
      super(name);
    }

    private MultiTask(String name, int sequence) {
      super(name, sequence);
    }

    private MultiTask(String name, String sequence) {
      super(name, Integer.parseInt(sequence));
    }

    @Override
    protected String getPrefix() {
      return PREFIX;
    }

    public static MultiTask create(String name) {
      return new MultiTask(name);
    }

    public static MultiTask create(String name, int sequence) {
      return new MultiTask(name, sequence);
    }
  }
}
