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

package gobblin.hive.spec.activity;

import java.io.IOException;

import gobblin.hive.HiveRegister;
import lombok.AllArgsConstructor;


/**
 * An {@link Activity} that drops a collection of Hive tables given a {@link HiveRegister}.
 *
 * @author ziliu
 */
@AllArgsConstructor
public class DropTableActivity implements Activity {

  protected final String dbName;
  protected final String tableName;

  @Override
  public boolean execute(HiveRegister register) throws IOException {
    register.dropTableIfExists(this.dbName, tableName);
    return true;
  }

}
