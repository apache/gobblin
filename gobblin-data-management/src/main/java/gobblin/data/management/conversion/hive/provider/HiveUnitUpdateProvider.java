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
package gobblin.data.management.conversion.hive.provider;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * An interface that finds when new data was added into a {@link Partition} or a {@link Table}
 */
public interface HiveUnitUpdateProvider {

  /**
   * Get the data update time of a {@link Partition}
   */
  public long getUpdateTime(Partition partition) throws UpdateNotFoundException;

  /**
   * Get the data update time of a {@link Table}
   */
  public long getUpdateTime(Table table) throws UpdateNotFoundException;

}
