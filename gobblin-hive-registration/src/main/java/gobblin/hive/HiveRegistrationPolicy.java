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

import java.util.Collection;

import com.google.common.base.Optional;


/**
 * An interface for specifying how to register a {@link HiveRegistrable}.
 *
 * @author ziliu
 */
public interface HiveRegistrationPolicy {

  /**
   * The Hive database name for registering a {@link HiveRegistrable}.
   */
  public String getDatabaseName(HiveRegistrable registrable);

  /**
   * The Hive table name for registering a {@link HiveRegistrable}.
   */
  public String getTableName(HiveRegistrable registrable);

  /**
   * Location of the table corresponding to the given {@link HiveRegistrable}.
   *
   * If the given {@link HiveRegistrable} represents a table, this method should return
   * {@link HiveRegistrable#getPath()}. If it represents a partition, this method should return
   * the location of the table where this partition belongs to.
   */
  public String getTableLocataion(HiveRegistrable registrable);

  /**
   * Get the {@link HivePartition} corresponding to the given {@link HiveRegistrable}.
   *
   * If the given {@link HiveRegistrable} represents a table, this method should return
   * {@link Optional#absent()}.
   */
  public Optional<HivePartition> getPartition(HiveRegistrable registrable);

  /**
   * Get a list of tables that should be dropped after registering the given {@link HiveRegistrable}.
   */
  public Collection<String> getObsoleteTableNames(HiveRegistrable registrable);

  /**
   * Get a list of partitions that should be dropped after registering the given {@link HiveRegistrable}.
   *
   * For example, after registering a daily partition of a dataset, one may want to drop the corresponding
   * hourly partitions.
   */
  public Collection<HivePartition> getObsoletePartitions(HiveRegistrable registrable);

  /**
   * Get a list of tables, such that if any of these tables already exists, the given {@link HiveRegistrable}
   * should not be registered.
   */
  public Collection<String> getDominatingTableNames(HiveRegistrable registrable);

  /**
   * Get a list of {@link HivePartition}s, such that if any of these {@link HivePartition}s already exists,
   * the given {@link HiveRegistrable} should not be registered.
   *
   * For example, one may want to register an hourly partition of a dataset only if the corresponding daily
   * partition does not exist.
   */
  public Collection<HivePartition> getDominatingPartitions(HiveRegistrable registrable);

}
