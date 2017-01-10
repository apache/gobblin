/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.compliance;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import gobblin.util.WriterUtils;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;


/**
 * Initializes {@link HivePurgerWriter} with {@link FileSystem} and {@link HivePurgerQueryExecutor}
 *
 * @author adsharma
 */
public class HivePurgerWriterBuilder extends DataWriterBuilder<ComplianceRecordSchema, ComplianceRecord> {

  @Override
  public DataWriter<ComplianceRecord> build()
      throws IOException {
    try {
      return new HivePurgerWriter(WriterUtils.getWriterFS(this.destination.getProperties(), this.branches, this.branch), new HivePurgerQueryExecutor());
    } catch (IOException | SQLException e) {
      throw new IOException(e);
    }
  }
}
