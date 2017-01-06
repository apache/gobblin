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
import java.util.Comparator;
import java.util.Iterator;


/**
 * A {@link Requestor} that can provide an {@link Iterator} of {@link Request}s already sorted by the input
 * prioritizer. Allows push down of certain prioritizers to more efficient layers.
 */
public interface PushDownRequestor<T extends Request> extends Requestor<T> {
  /**
   * Return an {@link Iterator} of {@link Request}s already sorted by the input prioritizer.
   */
  Iterator<T> getRequests(Comparator<T> prioritizer) throws IOException;
}
