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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;


/**
 * Stores the results of a RowLevelPolicy
 * @author stakiar
 */
public class RowLevelPolicyCheckResults {
  Map<RowLevelPolicyResultPair, Long> results;

  public RowLevelPolicyCheckResults() {
    this.results = Maps.newConcurrentMap();
  }

  public void put(RowLevelPolicy policy, RowLevelPolicy.Result result) {
    RowLevelPolicyResultPair resultPolicyPair = new RowLevelPolicyResultPair(policy, result);
    long value;
    if (this.results.containsKey(resultPolicyPair)) {
      value = this.results.get(resultPolicyPair);
    } else {
      value = 0;
    }
    this.results.put(new RowLevelPolicyResultPair(policy, result), Long.valueOf(1 + value));
  }

  public String getResults() {
    List<String> list = new ArrayList<>();
    Joiner joiner = Joiner.on("\n").skipNulls();
    for (Map.Entry<RowLevelPolicyResultPair, Long> entry : this.results.entrySet()) {
      list.add("RowLevelPolicy " + entry.getKey().getPolicy().toString() + " processed " + entry.getValue()
          + " record(s) with result " + entry.getKey().getResult());
    }
    return joiner.join(list);
  }

  public static class RowLevelPolicyResultPair {
    private RowLevelPolicy policy;
    private RowLevelPolicy.Result result;

    public RowLevelPolicyResultPair(RowLevelPolicy policy, RowLevelPolicy.Result result) {
      this.policy = policy;
      this.result = result;
    }

    public RowLevelPolicy getPolicy() {
      return this.policy;
    }

    public RowLevelPolicy.Result getResult() {
      return this.result;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof RowLevelPolicyResultPair)) {
        return false;
      }
      RowLevelPolicyResultPair p = (RowLevelPolicyResultPair) o;
      return p.getPolicy().toString().equals(this.policy.toString()) && p.getResult().equals(this.result);
    }

    @Override
    public int hashCode() {
      return (this.policy.toString() + this.result).hashCode();
    }
  }
}
