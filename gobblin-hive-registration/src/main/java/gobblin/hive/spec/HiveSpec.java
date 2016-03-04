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

package gobblin.hive.spec;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

import gobblin.annotation.Alpha;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveTable;


/**
 * An specification for Hive registration.
 */
@Alpha
public interface HiveSpec {

  /**
   * Get the {@link Path} to be registered in Hive.
   */
  public Path getPath();

  /**
   * Get the Hive {@link HiveTable} that the {@link Path} returned by {@link #getPath()} should be registered to.
   */
  public HiveTable getTable();

  /**
   * Get the Hive {@link HivePartition} that the {@link Path} returned by {@link #getPath()} should be registered to.
   *
   * @return {@link Optional#absent()} indicates the {@link Path} in this HiveSpec should be registered as
   * a Hive table. Otherwise, the {@link Path} should be registered as a Hive partition.
   */
  public Optional<HivePartition> getPartition();
}
