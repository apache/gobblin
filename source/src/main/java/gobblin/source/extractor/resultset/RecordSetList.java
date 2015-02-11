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
  private List<D> list = new ArrayList<D>();

  @Override
  public Iterator<D> iterator() {
    return this.list.iterator();
  }

  @Override
  public void add(D record) {
    list.add(record);
  }

  @Override
  public boolean isEmpty() {
    if (this.list.size() == 0) {
      return true;
    }
    return false;
  }
}
