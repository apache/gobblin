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

package gobblin.util.concurrent;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * The wrapper around a {@link ScheduledTask} whose execution can be cancelled.
 *
 * @param <K> the type of the key of the {@link ScheduledTask}
 * @param <T> the type of the {@link ScheduledTask}
 * @author joelbaranick
 */
@AllArgsConstructor
abstract class CancellableTask<K, T extends ScheduledTask<K>> {
    @Getter
    private T scheduledTask;

    /**
     * Attempts to cancel execution of this task. If the task
     * has been executed or cancelled already, it will return
     * with no side effect.
     *
     * @return true if the task was cancelled; otherwise, false
     */
    abstract boolean cancel();
}