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

import org.apache.hadoop.hive.metastore.api.SerDeInfo;

import gobblin.configuration.State;


/**
 * A {@link HiveSchemaManager} that does nothing. This should be used when registering a table
 * that does not require specifying schema properties.
 *
 * @author ziliu
 */
public class HiveNopSchemaManager extends HiveSchemaManager {

  protected HiveNopSchemaManager(State props) {
    super(props);
  }

  @Override
  public void addSchemaProperties(SerDeInfo si, HiveRegistrable registrable) {
    // Do nothing
  }

}
