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
import gobblin.source.workunit.WorkUnitStream;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.DataWriterBuilder;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Factory method pattern class provides WriterInitializer based on writer and state.
 */
public class WriterInitializerFactory {
  /**
   * Provides WriterInitializer based on the writer. Mostly writer is decided by the Writer builder (and destination) that user passes.
   * If there's more than one branch, it will instantiate same number of WriterInitializer instance as number of branches and combine it into MultiWriterInitializer.
   *
   * @param state
   * @return WriterInitializer
   */
  public static WriterInitializer newInstace(State state, WorkUnitStream workUnits) {
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

  private static WriterInitializer newSingleInstance(State state, WorkUnitStream workUnits, int branches, int branchId) {
    Preconditions.checkNotNull(state);

    String writerBuilderKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUILDER_CLASS, branches, branchId);
    String writerBuilderClass = state.getProp(writerBuilderKey, ConfigurationKeys.DEFAULT_WRITER_BUILDER_CLASS);

    DataWriterBuilder dataWriterBuilder;
    try {
      dataWriterBuilder = (DataWriterBuilder) Class.forName(writerBuilderClass).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return dataWriterBuilder.getInitializer(state, workUnits, branches, branchId);
  }
}
