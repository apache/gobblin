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

package gobblin.source.extractor.resultset;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * An implementation of RecordSet to return data records through an interator
 *
 * @param <D> type of data record
 */
public class RecordSetList<D> implements RecordSet<D> {
  private List<D> list = new ArrayList<>();

  @Override
  public Iterator<D> iterator() {
    return this.list.iterator();
  }

  @Override
  public void add(D record) {
    this.list.add(record);
  }

  @Override
  public boolean isEmpty() {
    if (this.list.size() == 0) {
      return true;
    }
    return false;
  }
}
