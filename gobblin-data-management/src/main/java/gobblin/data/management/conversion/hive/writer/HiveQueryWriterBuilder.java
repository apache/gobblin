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
package gobblin.data.management.conversion.hive.writer;

import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.avro.Schema;

import gobblin.hive.util.HiveJdbcConnector;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

/**
 *  A {@link DataWriterBuilder} for {@link HiveQueryWriterBuilder}
 */
public class HiveQueryWriterBuilder extends DataWriterBuilder<Schema, QueryBasedHiveConversionEntity>{

  @Override
  public DataWriter<QueryBasedHiveConversionEntity> build() throws IOException {
    try {
      return new HiveQueryExecutionWriter(HiveJdbcConnector.newConnectorWithProps(this.destination.getProperties().getProperties()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
