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
package org.apache.gobblin.source;

import com.google.common.eventbus.EventBus;
import org.apache.gobblin.annotation.Alpha;


/**
 * An interface for infinite source, where source should be able to detect the work unit change
 * and post the change through eventBus
 *
 * @author Zihan Li
 *
 * @param <S> output schema type
 * @param <D> output record type
 */
@Alpha
public interface InfiniteSource<S, D> extends Source<S, D>{

  /**
   * Return the eventBus where it will post {@link org.apache.gobblin.stream.WorkUnitChangeEvent} when workUnit change
   */
  EventBus getEventBus();

}
