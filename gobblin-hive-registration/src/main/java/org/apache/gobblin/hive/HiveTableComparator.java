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

package gobblin.hive;

/**
 * An extension to {@link HiveRegistrationUnitComparator} for {@link HiveTable}s.
 *
 * @author Ziyang Liu
 */
public class HiveTableComparator<T extends HiveTableComparator<?>> extends HiveRegistrationUnitComparator<T> {

  public HiveTableComparator(HiveTable existingTable, HiveTable newTable) {
    super(existingTable, newTable);
  }

  @SuppressWarnings("unchecked")
  public T compareOwner() {
    if (!this.result) {
      compare(((HiveTable) this.existingUnit).getOwner(), ((HiveTable) this.newUnit).getOwner());
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T compareRetention() {
    if (!this.result) {
      compare(((HiveTable) this.existingUnit).getRetention(), ((HiveTable) this.newUnit).getRetention());
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T compareAll() {
    super.compareAll().compareOwner().compareRetention();
    return (T) this;
  }
}
