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

package gobblin.hive.spec.predicate;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;

import gobblin.hive.HiveRegister;
import gobblin.hive.HiveRegistrationUnit.Column;
import lombok.AllArgsConstructor;


/**
 * A {@link Predicate} that returns true if the given Hive partition does not exist.
 *
 * @author Ziyang Liu
 */
@AllArgsConstructor
public class PartitionNotExistPredicate implements Predicate<HiveRegister> {

  protected final String dbName;
  protected final String tableName;
  protected final List<Column> partitionKeys;
  protected final List<String> partitionValues;

  @Override
  public boolean apply(HiveRegister register) {
    try {
      return !register.existsPartition(this.dbName, this.tableName, this.partitionKeys, this.partitionValues);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
