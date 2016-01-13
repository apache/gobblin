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

package gobblin.hive.avro;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;

import gobblin.configuration.State;
import gobblin.hive.HiveRegistrable;
import gobblin.hive.HiveSchemaManager;


/**
 * A {@link HiveSchemaManager} for registering Avro tables and partitions.
 *
 * @author ziliu
 */
//TODO this class is not finished.
public class HiveAvroSchemaManager extends HiveSchemaManager {

  protected HiveAvroSchemaManager(State props) {
    super(props);
  }

  @Override
  public void addSchemaProperties(SerDeInfo si, HiveRegistrable registrable) {
    // TODO
  }

}
