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
package org.apache.gobblin.salesforce;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A class encapsulating the count of records in the consecutive intervals.
 */
@Getter
public class Histogram {
  private long totalRecordCount;
  private final List<Group> groups;

  public Histogram() {
    totalRecordCount = 0;
    groups = new ArrayList<>();
  }

  void add(Group group) {
    groups.add(group);
    totalRecordCount += group.getCount();
  }

  void add(Histogram histogram) {
    groups.addAll(histogram.getGroups());
    totalRecordCount += histogram.totalRecordCount;
  }

  Group get(int idx) {
    return this.groups.get(idx);
  }

  @Override
  public String toString() {
    return groups.toString();
  }

  /**
   * A class to encapsulate the key and the corresponding frequency/count, in the context of a
   * histogram. It represents one data point in the histogram.
   */
  @Getter
  @AllArgsConstructor
  static class Group {
    private final String key;
    private final int count;

    @Override
    public String toString() {
      return key + ":" + count;
    }
  }
}
