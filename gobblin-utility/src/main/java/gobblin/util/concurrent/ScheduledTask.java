/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.concurrent;

/**
 * A task which can be scheduled to run recurrently by an instance of {@link TaskScheduler}.
 *
 * @author joelbaranick
 */
public interface ScheduledTask<K> {
    /**
     * The unique key used to identify this task.
     *
     * @return the unique key
     */
    K getKey();

    /**
     * Run one iteration of the scheduled task.
     */
    void runOneIteration();
}
