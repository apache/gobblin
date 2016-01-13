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

package gobblin.hive.policy;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HivePartition;
import gobblin.hive.spec.HiveSpec;
import gobblin.hive.spec.SimpleHiveSpec;


/**
 * A base implementation of {@link HiveRegistrationPolicy}. It obtains database name from
 * property {@link #HIVE_DATABASE_NAME} or {@link #HIVE_DATABASE_REGEX} (group 1), obtains
 * table name from property {@link #HIVE_TABLE_NAME} and {@link #HIVE_TABLE_REGEX} (group 1),
 * and builds a {@link SimpleHiveSpec}.
 *
 * @author ziliu
 */
@Alpha
public abstract class HiveRegistrationPolicyBase implements HiveRegistrationPolicy {

  public static final String HIVE_REGISTRATION_POLICY = "hive.registration.policy";

  public static final String HIVE_DATABASE_NAME = "hive.database.name";
  public static final String HIVE_DATABASE_REGEX = "hive.database.regex";
  public static final String HIVE_TABLE_NAME = "hive.table.name";
  public static final String HIVE_TABLE_REGEX = "hive.table.regex";
  public static final String HIVE_SANITIZE_INVALID_NAMES = "hive.sanitize.invalid.names";

  /**
   * A valid db or table name should start with an alphanumeric character, and contains only
   * alphanumeric characters and '_'.
   */
  private static final Pattern VALID_DB_TABLE_NAME_PATTERN_1 = Pattern.compile("[a-z0-9][a-z0-9_]*");

  /**
   * A valid db or table name should contain at least one letter or '_' (i.e., should not be numbers only).
   */
  private static final Pattern VALID_DB_TABLE_NAME_PATTERN_2 = Pattern.compile(".*[a-z_].*");

  protected final State props;
  protected final boolean sanitizeNameAllowed;
  protected final Optional<Pattern> dbNamePattern;
  protected final Optional<Pattern> tableNamePattern;

  protected HiveRegistrationPolicyBase(State props) {
    Preconditions.checkNotNull(props);
    this.props = props;
    this.sanitizeNameAllowed = props.getPropAsBoolean(HIVE_SANITIZE_INVALID_NAMES, true);
    this.dbNamePattern = props.contains(HIVE_DATABASE_REGEX)
        ? Optional.of(Pattern.compile(props.getProp(HIVE_DATABASE_REGEX))) : Optional.<Pattern> absent();
    this.tableNamePattern = props.contains(HIVE_TABLE_REGEX)
        ? Optional.of(Pattern.compile(props.getProp(HIVE_TABLE_REGEX))) : Optional.<Pattern> absent();
  }

  /**
   * This method first tries to obtain the database name from {@link #HIVE_DATABASE_NAME}.
   * If this property is not specified, it then tries to obtain the database name using
   * the first group of {@link #HIVE_DATABASE_REGEX}.
   */
  protected String getDatabaseName(Path path) {
    return getDatabaseOrTableName(path, HIVE_DATABASE_NAME, HIVE_DATABASE_REGEX, this.dbNamePattern);
  }

  /**
   * This method first tries to obtain the database name from {@link #HIVE_TABLE_NAME}.
   * If this property is not specified, it then tries to obtain the database name using
   * the first group of {@link #HIVE_TABLE_REGEX}.
   */
  protected String getTableName(Path path) {
    return getDatabaseOrTableName(path, HIVE_TABLE_NAME, HIVE_TABLE_REGEX, this.tableNamePattern);
  }

  protected String getDatabaseOrTableName(Path path, String nameKey, String regexKey, Optional<Pattern> pattern) {
    String name = null;
    if (this.props.contains(nameKey)) {
      name = this.props.getProp(nameKey);
    } else if (pattern.isPresent()) {
      name = pattern.get().matcher(path.toString()).group();
    } else {
      throw new IllegalStateException("Missing required property " + nameKey + " or " + regexKey);
    }

    name = name.toLowerCase();

    if (this.sanitizeNameAllowed && !isNameValid(name)) {
      name = sanitizeName(name);
    }

    if (isNameValid(name)) {
      return name;
    }

    throw new IllegalStateException(name + " is not a valid Hive database or table name");
  }

  protected abstract Optional<HivePartition> getPartition(Path path);

  protected abstract StorageDescriptor getSd(Path path) throws IOException;

  /**
   * Determine whether a database or table name is valid.
   *
   * A name is valid if and only if: it starts with an alphanumeric character, contains only alphanumeric characters
   * and '_', and is NOT composed of numbers only.
   */
  protected static boolean isNameValid(String name) {
    Preconditions.checkNotNull(name);
    name = name.toLowerCase();
    return VALID_DB_TABLE_NAME_PATTERN_1.matcher(name).matches()
        && VALID_DB_TABLE_NAME_PATTERN_2.matcher(name).matches();
  }

  /**
   * Attempt to sanitize an invalid database or table name by replacing characters that are not alphanumeric
   * or '_' with '_'.
   */
  protected static String sanitizeName(String name) {
    return name.replaceAll("[^a-zA-Z0-9_]", "_");
  }

  @Override
  public HiveSpec getHiveSpec(Path path) throws IOException {
    return new SimpleHiveSpec.Builder<>(path).withDbName(getDatabaseName(path)).withTableName(getTableName(path))
        .withPartition(getPartition(path)).withStorageDescriptor(getSd(path)).build();
  }

  /**
   * Get a {@link HiveRegistrationPolicy} from a {@link State} object.
   *
   * @param props A {@link State} object that contains property, {@link #HIVE_REGISTRATION_POLICY},
   * which is the class name of the desired policy. This policy class must have a constructor that
   * takes a {@link State} object.
   */
  public static HiveRegistrationPolicy getPolicy(State props) {
    Preconditions.checkArgument(props.contains(HIVE_REGISTRATION_POLICY));

    String policyType = props.getProp(HIVE_REGISTRATION_POLICY);
    try {
      return (HiveRegistrationPolicy) ConstructorUtils.invokeConstructor(Class.forName(policyType), props);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(
          "Unable to instantiate " + HiveRegistrationPolicy.class.getSimpleName() + " with type " + policyType, e);
    }
  }
}
