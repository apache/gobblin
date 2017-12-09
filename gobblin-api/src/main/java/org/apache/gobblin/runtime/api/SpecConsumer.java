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

package org.apache.gobblin.runtime.api;

import java.util.List;
import java.util.concurrent.Future;
import org.apache.commons.lang3.tuple.Pair;


/**
 * A communication socket (receiving side for this class)
 * for each {@link SpecExecutor} to receive spec to execute from Orchestrator.
 * Implementation of this interface should specify communication channel (e.g. Kafka, REST, etc.)
 */
public interface SpecConsumer<V>  {

  /** List of newly changed {@link Spec}s for execution on {@link SpecExecutor}. */
  Future<? extends List<Pair<SpecExecutor.Verb, V>>> changedSpecs();

}