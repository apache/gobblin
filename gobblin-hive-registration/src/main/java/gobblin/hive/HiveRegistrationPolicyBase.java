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

package gobblin.hive;

import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import gobblin.configuration.State;


/**
 * A base implementation of {@link HiveRegistrationPolicy}.
 *
 * @author ziliu
 */
public abstract class HiveRegistrationPolicyBase implements HiveRegistrationPolicy {

  public static final String HIVE_DATABASE_NAME = "hive.database.name";
  public static final String HIVE_DATABASE_REGEX = "hive.database.regex";
  public static final String HIVE_TABLE_NAME = "hive.table.name";
  public static final String HIVE_TABLE_REGEX = "hive.table.regex";

  private static final Pattern NAME_PATTERN_1 = Pattern.compile("[a-z0-9][a-z0-9_]*)");
  private static final Pattern NAME_PATTERN_2 = Pattern.compile(".*[a-z_].*");

  protected final State props;

  protected HiveRegistrationPolicyBase(State props) {
    Preconditions.checkNotNull(props);
    this.props = props;
  }

  /**
   * This method first tries to obtain the database name from {@link #HIVE_DATABASE_NAME}.
   * If this property is not specified, it then tries to obtain the database name using
   * the first group of {@link #HIVE_DATABASE_REGEX}.
   */
  @Override
  public String getDatabaseName(HiveRegistrable registrable) {
    return getDatabaseOrTableName(registrable, HIVE_DATABASE_NAME, HIVE_DATABASE_REGEX);
  }

  /**
   * This method first tries to obtain the database name from {@link #HIVE_TABLE_NAME}.
   * If this property is not specified, it then tries to obtain the database name using
   * the first group of {@link #HIVE_TABLE_REGEX}.
   */
  @Override
  public String getTableName(HiveRegistrable registrable) {
    return getDatabaseOrTableName(registrable, HIVE_TABLE_NAME, HIVE_TABLE_REGEX);
  }

  protected String getDatabaseOrTableName(HiveRegistrable registrable, String nameKey, String regexKey) {
    String name = null;
    if (this.props.contains(nameKey)) {
      name = this.props.getProp(nameKey);
    } else if (this.props.contains(regexKey)) {
      String regex = this.props.getProp(regexKey);
      name = Pattern.compile(regex).matcher(registrable.getPath().toString()).group();
    }

    if (isNameValid(name)) {
      return name;
    }

    throw new IllegalStateException(name + " is not a valid Hive database or table name");
  }

  /**
   * Determine whether a database or table name is valid.
   *
   * A name is valid if and only if: it starts with an alphanumeric character, contains only alphanumeric characters
   * and '_', and is NOT composed of numbers only.
   */
  protected static boolean isNameValid(String name) {
    Preconditions.checkNotNull(name);
    name = name.toLowerCase();
    return NAME_PATTERN_1.matcher(name).matches() && NAME_PATTERN_2.matcher(name).matches();
  }
}
