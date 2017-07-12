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


package gobblin.data.management.dataset;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.Iterators;

import lombok.AllArgsConstructor;
import lombok.Getter;

import gobblin.dataset.Dataset;
import gobblin.util.request_allocation.PushDownRequestor;

/**
 * A simple {@link gobblin.util.request_allocation.Requestor} used to generate a single {@link SimpleDatasetRequest}
 */
@AllArgsConstructor
public class SimpleDatasetRequestor implements PushDownRequestor<SimpleDatasetRequest> {
  @Getter
  private Dataset dataset;

  @Override
  public Iterator<SimpleDatasetRequest> getRequests(Comparator<SimpleDatasetRequest> prioritizer)
      throws IOException {
    return Iterators.singletonIterator(new SimpleDatasetRequest(dataset, this));
  }

  @Override
  public Iterator<SimpleDatasetRequest> iterator() {
    return Iterators.singletonIterator(new SimpleDatasetRequest(dataset, this));
  }
}
