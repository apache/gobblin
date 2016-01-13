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
import java.util.Collection;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;

import gobblin.hive.HiveRegister;
import lombok.AllArgsConstructor;


/**
 * A {@link Predicate} that returns true if none of a collection of Hive tables exists
 * in a {@link HiveRegister}.
 *
 * @author ziliu
 */
@AllArgsConstructor
public class TablesNotExistPredicate implements Predicate<HiveRegister> {

  protected final String dbName;
  protected final Collection<String> tableNames;

  @Override
  public boolean apply(HiveRegister register) {
    for (String tableName : this.tableNames) {
      try {
        if (register.existsTable(this.dbName, tableName)) {
          return false;
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return true;
  }

}
