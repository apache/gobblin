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

package gobblin.qualitychecker.row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;


/**
 * Stores the results of a RowLevelPolicy
 * @author stakiar
 */
public class RowLevelPolicyCheckResults {
  Map<RowLevelPolicyResultPair, Long> results;

  public RowLevelPolicyCheckResults() {
    results = new HashMap<RowLevelPolicyResultPair, Long>();
  }

  public void put(RowLevelPolicy policy, RowLevelPolicy.Result result) {
    RowLevelPolicyResultPair resultPolicyPair = new RowLevelPolicyResultPair(policy, result);
    long value;
    if (results.containsKey(resultPolicyPair)) {
      value = results.get(resultPolicyPair);
    } else {
      value = 0;
    }
    results.put(new RowLevelPolicyResultPair(policy, result), new Long(1 + value));
  }

  public String getResults() {
    List<String> list = new ArrayList<String>();
    Joiner joiner = Joiner.on("\n").skipNulls();
    for (Map.Entry<RowLevelPolicyResultPair, Long> entry : results.entrySet()) {
      list.add("RowLevelPolicy " + entry.getKey().getPolicy().toString() + " processed " + entry.getValue()
          + " record(s) with result " + entry.getKey().getResult());
    }
    return joiner.join(list);
  }

  public class RowLevelPolicyResultPair {
    private RowLevelPolicy policy;
    private RowLevelPolicy.Result result;

    public RowLevelPolicyResultPair(RowLevelPolicy policy, RowLevelPolicy.Result result) {
      this.policy = policy;
      this.result = result;
    }

    public RowLevelPolicy getPolicy() {
      return policy;
    }

    public RowLevelPolicy.Result getResult() {
      return result;
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
