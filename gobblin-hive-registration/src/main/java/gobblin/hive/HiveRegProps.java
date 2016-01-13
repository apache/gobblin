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

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import lombok.Getter;


/**
 * An extension to {@link State} for Hive registration.
 *
 * @author ziliu
 */
@Alpha
public class HiveRegProps extends State {

  public static final String HIVE_DB_ROOT_DIR = "hive.db.root.dir";
  public static final String HIVE_OWNER = "hive.owner";
  public static final String HIVE_REGISTER_THREADS = "hive.register.threads";
  public static final int DEFAULT_HIVE_REGISTER_THREADS = 5;
  public static final String HIVE_TABLE_PROPS = "hive.table.props";

  private static final Splitter SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();

  @Getter
  private final State tableProps;

  /**
   * @param props A {@link State} object that includes both properties required by {@link HiveRegister} to do
   * Hive registration, as well as the Hive properties that will be added to the Hive table when creating the table,
   * e.g., orc.compress=SNAPPY
   *
   * <p>
   *   The Hive table properties should be a comma-separated list associated with {@link #HIVE_TABLE_PROPS} in the
   *   given {@link State}.
   * </p>
   */
  public HiveRegProps(State props) {
    super(props);
    this.tableProps = createTableProps();
  }

  /**
   * @param props Properties required by {@link HiveRegister} to do Hive registration
   * @param tableProps Hive properties that will be added to the Hive table when creating the table,
   * e.g., orc.compress=SNAPPY
   */
  public HiveRegProps(State props, State tableProps) {
    super(props);
    this.tableProps = tableProps;
  }

  /**
   * Create a {@link State} object that contains Hive table properties. These properties are obtained from
   * {@link #HIVE_TABLE_PROPS}, which is a list of comma-separated properties. Each property is in the form
   * of '[key]=[value]'.
   */
  private State createTableProps() {
    State state = new State();

    if (!contains(HIVE_TABLE_PROPS)) {
      return state;
    }

    for (String tableProp : getPropAsList(HIVE_TABLE_PROPS)) {
      List<String> tokens = SPLITTER.splitToList(tableProp);

      Preconditions.checkState(tokens.size() == 2, tableProp + " is not a valid Hive table property");
      state.setProp(tokens.get(0), tokens.get(1));
    }
    return state;
  }

  /**
   * Get Hive database root dir from {@link #HIVE_DB_ROOT_DIR}.
   *
   * @return {@link Optional#absent()} if {@link #HIVE_DB_ROOT_DIR} is not specified.
   */
  public Optional<String> getDbRootDir() {
    return Optional.fromNullable(getProp(HIVE_DB_ROOT_DIR));
  }

  /**
   * Get Hive owner from {@link #HIVE_OWNER}.
   *
   * @return {@link Optional#absent()} if {@link #HIVE_OWNER} is not specified.
   */
  public Optional<String> getHiveOwner() {
    return Optional.fromNullable(getProp(HIVE_OWNER));
  }

  /**
   * Get number of threads from {@link #HIVE_REGISTER_THREADS}, with a default value of
   * {@link #DEFAULT_HIVE_REGISTER_THREADS}.
   */
  public int getNumThreads() {
    return getPropAsInt(HIVE_REGISTER_THREADS, DEFAULT_HIVE_REGISTER_THREADS);
  }

}
