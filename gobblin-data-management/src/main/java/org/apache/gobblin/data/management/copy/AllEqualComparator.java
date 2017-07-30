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

package gobblin.data.management.copy;

import java.io.Serializable;
import java.util.Comparator;


/**
 * Implementation of {@link java.util.Comparator} where all elements are equal.
 */
public class AllEqualComparator<T> implements Comparator<T>, Serializable {

  private static final long serialVersionUID = 5144295901248792907L;

  @Override public int compare(T o1, T o2) {
    return 0;
  }
}
