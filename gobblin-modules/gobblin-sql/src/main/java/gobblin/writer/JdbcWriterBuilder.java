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

package gobblin.writer;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntrySchema;
import gobblin.source.workunit.WorkUnitStream;
import gobblin.writer.commands.JdbcWriterCommandsFactory;
import gobblin.writer.initializer.JdbcWriterInitializer;
import gobblin.writer.initializer.WriterInitializer;

import java.io.IOException;

public class JdbcWriterBuilder extends DataWriterBuilder<JdbcEntrySchema, JdbcEntryData> {

  @Override
  public DataWriter<JdbcEntryData> build() throws IOException {
    return new JdbcWriter(this);
  }

  public WriterInitializer getInitializer(State state, WorkUnitStream workUnits, int branches, int branchId) {
    JdbcWriterCommandsFactory factory = new JdbcWriterCommandsFactory();
    if (workUnits.isSafeToMaterialize()) {
      return new JdbcWriterInitializer(state, workUnits.getMaterializedWorkUnitCollection(),
          factory, branches, branchId);
    } else {
      throw new RuntimeException(JdbcWriterBuilder.class.getName() + " does not support work unit streams.");
    }
  }
}