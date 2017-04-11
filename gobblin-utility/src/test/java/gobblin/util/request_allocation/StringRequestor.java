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

package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
public class StringRequestor implements PushDownRequestor<StringRequest> {

  public StringRequestor(String name, String... strings) {
    this(name, Lists.newArrayList(strings));
  }

  @Getter
  private final String name;
  private final List<String> strings;

  @Override
  public Iterator<StringRequest> iterator() {
    return Iterators.transform(this.strings.iterator(), new Function<String, StringRequest>() {
      @Override
      public StringRequest apply(String input) {
        return new StringRequest(StringRequestor.this, input);
      }
    });
  }

  @Override
  public Iterator<StringRequest> getRequests(Comparator<StringRequest> prioritizer)
      throws IOException {
    List<StringRequest> requests = Lists.newArrayList(Iterators.transform(this.strings.iterator(), new Function<String, StringRequest>() {
      @Override
      public StringRequest apply(String input) {
        return new StringRequest(StringRequestor.this, input);
      }
    }));
    Collections.sort(requests, prioritizer);
    return requests.iterator();
  }
}
