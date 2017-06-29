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

package gobblin.qualitychecker.row;

import gobblin.configuration.ConfigurationKeys;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;


public class RowLevelPolicyCheckerBuilder {
  private final State state;
  private final int index;

  private static final Logger LOG = LoggerFactory.getLogger(RowLevelPolicyCheckerBuilder.class);

  public RowLevelPolicyCheckerBuilder(State state, int index) {
    this.state = state;
    this.index = index;
  }

  @SuppressWarnings("unchecked")
  private List<RowLevelPolicy> createPolicyList()
      throws Exception {
    List<RowLevelPolicy> list = new ArrayList<>();
    Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
    String rowLevelPoliciesKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, this.index);
    String rowLevelPolicyTypesKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, this.index);
    if (this.state.contains(rowLevelPoliciesKey) && this.state.contains(rowLevelPolicyTypesKey)) {
      List<String> policies = Lists.newArrayList(splitter.split(this.state.getProp(rowLevelPoliciesKey)));
      List<String> types = Lists.newArrayList(splitter.split(this.state.getProp(rowLevelPolicyTypesKey)));
      if (policies.size() != types.size()) {
        throw new Exception("Row Policies list and Row Policies list type are not the same length");
      }
      for (int i = 0; i < policies.size(); i++) {
        try {
          Class<? extends RowLevelPolicy> policyClass =
              (Class<? extends RowLevelPolicy>) Class.forName(policies.get(i));
          Constructor<? extends RowLevelPolicy> policyConstructor =
              policyClass.getConstructor(State.class, RowLevelPolicy.Type.class);
          RowLevelPolicy policy = policyConstructor.newInstance(this.state, RowLevelPolicy.Type.valueOf(types.get(i)));
          list.add(policy);
        } catch (Exception e) {
          LOG.error(
              rowLevelPoliciesKey + " contains a class " + policies.get(i) + " which doesn't extend RowLevelPolicy.",
              e);
          throw e;
        }
      }
    }
    return list;
  }

  public static RowLevelPolicyCheckerBuilder newBuilder(State state, int index) {
    return new RowLevelPolicyCheckerBuilder(state, index);
  }

  public RowLevelPolicyChecker build()
      throws Exception {
    return new RowLevelPolicyChecker(createPolicyList(), this.state.getId(),
        WriterUtils.getWriterFS(this.state, 1, 0), this.state);
  }
}
