/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.qualitychecker.task;

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


/**
 * Creates a PolicyChecker and initializes the PolicyList
 * the list is Policies to create is taken from the
 * MetadataCollector
 */
public class TaskLevelPolicyCheckerBuilder {
  private final State state;
  private final int index;

  private static final Logger LOG = LoggerFactory.getLogger(TaskLevelPolicyCheckerBuilder.class);

  public TaskLevelPolicyCheckerBuilder(State state, int index) {
    this.state = state;
    this.index = index;
  }

  @SuppressWarnings("unchecked")
  private List<TaskLevelPolicy> createPolicyList()
      throws Exception {
    List<TaskLevelPolicy> list = new ArrayList<TaskLevelPolicy>();
    String taskLevelPoliciesKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, this.index);
    String taskLevelPolicyTypesKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, this.index);
    if (this.state.contains(taskLevelPoliciesKey) && this.state.contains(taskLevelPolicyTypesKey)) {
      Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
      List<String> policies = Lists.newArrayList(splitter.split(this.state.getProp(taskLevelPoliciesKey)));
      List<String> types = Lists.newArrayList(splitter.split(this.state.getProp(taskLevelPolicyTypesKey)));
      if (policies.size() != types.size()) {
        throw new Exception("TaskLevelPolicy list and TaskLevelPolicies type list are not the same length");
      }
      for (int i = 0; i < policies.size(); i++) {
        try {
          Class<? extends TaskLevelPolicy> policyClass =
              (Class<? extends TaskLevelPolicy>) Class.forName(policies.get(i));
          Constructor<? extends TaskLevelPolicy> policyConstructor =
              policyClass.getConstructor(State.class, TaskLevelPolicy.Type.class);
          TaskLevelPolicy policy =
              policyConstructor.newInstance(this.state, TaskLevelPolicy.Type.valueOf(types.get(i)));
          list.add(policy);
        } catch (Exception e) {
          LOG.error(taskLevelPoliciesKey + " contains a class " + policies.get(i) + " which doesn't extend Policy.", e);
          throw e;
        }
      }
    }
    return list;
  }

  public static TaskLevelPolicyCheckerBuilder newBuilder(State state, int index) {
    return new TaskLevelPolicyCheckerBuilder(state, index);
  }

  public TaskLevelPolicyChecker build()
      throws Exception {
    return new TaskLevelPolicyChecker(createPolicyList());
  }
}
