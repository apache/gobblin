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

package gobblin.converter.initializer;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.converter.Converter;
import gobblin.util.ForkOperatorUtils;
import gobblin.source.workunit.WorkUnitStream;


public class ConverterInitializerFactory {
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().omitEmptyStrings();

  /**
   * Provides WriterInitializer based on the writer. Mostly writer is decided by the Writer builder (and destination) that user passes.
   * If there's more than one branch, it will instantiate same number of WriterInitializer instance as number of branches and combine it into MultiWriterInitializer.
   *
   * @param state
   * @return WriterInitializer
   */
  public static ConverterInitializer newInstance(State state, WorkUnitStream workUnits) {
    int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    if (branches == 1) {
      return newInstance(state, workUnits, branches, 0);
    }

    List<ConverterInitializer> cis = Lists.newArrayList();
    for (int branchId = 0; branchId < branches; branchId++) {
      cis.add(newInstance(state, workUnits, branches, branchId));
    }
    return new MultiConverterInitializer(cis);
  }

  private static ConverterInitializer newInstance(State state, WorkUnitStream workUnits, int branches,
      int branchId) {
    Preconditions.checkNotNull(state);

    String converterClassesParam =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.CONVERTER_CLASSES_KEY, branches, branchId);
    List<String> converterClasses = COMMA_SPLITTER.splitToList(state.getProp(converterClassesParam, ""));

    if (converterClasses.isEmpty()) {
      return NoopConverterInitializer.INSTANCE;
    }

    List<ConverterInitializer> cis = Lists.newArrayList();
    for (String converterClass : converterClasses) {
      Converter converter;
      try {
        converter = (Converter) Class.forName(converterClass).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      cis.add(converter.getInitializer(state, workUnits, branches, branchId));
    }
    return new MultiConverterInitializer(cis);
  }
}
