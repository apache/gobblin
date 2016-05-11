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
package gobblin.data.management.convertion.hive;

import java.io.IOException;
import java.sql.SQLException;

import lombok.AllArgsConstructor;

import gobblin.hive.util.HiveJdbcConnector;
import gobblin.writer.DataWriter;

/**
 * The {@link HiveQueryExecutionWriter} is responsible for running the hive query available at
 * {@link QueryBasedHiveConversionEntity#getConversionQuery()}
 */
@AllArgsConstructor
public class HiveQueryExecutionWriter implements DataWriter<QueryBasedHiveConversionEntity> {

  private final HiveJdbcConnector hiveJdbcConnector;

  @Override
  public void write(QueryBasedHiveConversionEntity hiveConversionEntity) throws IOException {

    try {
      this.hiveJdbcConnector.executeStatements(hiveConversionEntity.getConversionQuery());
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void commit() throws IOException {}

  @Override
  public void close() throws IOException {
    this.hiveJdbcConnector.close();
  }

  @Override
  public void cleanup() throws IOException {}

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    return 0;
  }
}
