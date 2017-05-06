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
package gobblin.compliance;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

/**
 * Initializes {@link HivePurgerWriter} with {@link FileSystem} and {@link HivePurgerQueryExecutor}
 *
 * @author adsharma
 */
public class HivePurgerWriterBuilder extends DataWriterBuilder<HivePurgerPartitionRecordSchema, HivePurgerPartitionRecord> {

  @Override
  public DataWriter<HivePurgerPartitionRecord> build()
      throws IOException {
    try {
      return new HivePurgerWriter(FileSystem.get(new Configuration()), new HivePurgerQueryExecutor());
    } catch (IOException | SQLException e) {
      throw new IOException(e);
    }
  }
}
