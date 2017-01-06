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

package gobblin.writer.initializer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.JdbcWriterBuilder;
import gobblin.writer.commands.JdbcWriterCommandsFactory;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Factory method pattern class provides WriterInitializer based on writer and state.
 */
public class WriterInitializerFactory {
  private static final NoopWriterInitializer NOOP = new NoopWriterInitializer();

  /**
   * Provides WriterInitializer based on the writer. Mostly writer is decided by the Writer builder (and destination) that user passes.
   * If there's more than one branch, it will instantiate same number of WriterInitializer instance as number of branches and combine it into MultiWriterInitializer.
   *
   * @param state
   * @return WriterInitializer
   */
  public static WriterInitializer newInstace(State state, Collection<WorkUnit> workUnits) {
    int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    if (branches == 1) {
      return newSingleInstance(state, workUnits, branches, 0);
    }

    List<WriterInitializer> wis = Lists.newArrayList();
    for (int branchId = 0; branchId < branches; branchId++) {
      wis.add(newSingleInstance(state, workUnits, branches, branchId));
    }
    return new MultiWriterInitializer(wis);
  }

  private static WriterInitializer newSingleInstance(State state, Collection<WorkUnit> workUnits, int branches, int branchId) {
    Preconditions.checkNotNull(state);

    String writerBuilderKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUILDER_CLASS, branches, branchId);
    String writerBuilderClass = state.getProp(writerBuilderKey, ConfigurationKeys.DEFAULT_WRITER_BUILDER_CLASS);

    if(JdbcWriterBuilder.class.getName().equals(writerBuilderClass)) {
      return new JdbcWriterInitializer(state, workUnits, new JdbcWriterCommandsFactory(), branches, branchId);
    }
    return NOOP;
  }
}
