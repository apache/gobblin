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

package org.apache.gobblin.dataset.comparators;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.gobblin.dataset.URNIdentified;

import lombok.EqualsAndHashCode;


/**
 * Dataset comparator that compares by dataset urn.
 */
@EqualsAndHashCode
public class URNLexicographicalComparator implements Comparator<URNIdentified>, Serializable {
  private static final long serialVersionUID = 2647543651352156568L;

  @Override
  public int compare(URNIdentified o1, URNIdentified o2) {
    return o1.getUrn().compareTo(o2.getUrn());
  }

  /**
   * Compare against a raw URN.
   */
  public int compare(URNIdentified o1, String urn2) {
    return o1.getUrn().compareTo(urn2);
  }

  /**
   * Compare against a raw URN.
   */
  public int compare(String urn1, URNIdentified o2) {
    return urn1.compareTo(o2.getUrn());
  }
}
