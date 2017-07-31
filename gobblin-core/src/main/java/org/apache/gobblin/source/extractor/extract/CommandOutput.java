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

package org.apache.gobblin.source.extractor.extract;

import java.util.Map;


/**
 * Stores the output of a Command into a Map object
 * with types K and V
 * @author stakiar
 *
 * @param <K> the key type of the Map
 * @param <V> the value type of the Map
 */
public interface CommandOutput<K extends Command, V> {
  public void storeResults(Map<K, V> results);

  public Map<K, V> getResults();

  public void put(K key, V value);
}
