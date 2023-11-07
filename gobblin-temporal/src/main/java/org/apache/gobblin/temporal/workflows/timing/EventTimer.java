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
package org.apache.gobblin.temporal.workflows.timing;

import java.io.Closeable;

import org.apache.gobblin.metrics.event.TimingEvent;


/**
 * A timer that can be used to track the duration of an event. This event differs from the {@link TimingEvent} in that
 * this class is not meant to be used outside of {@link io.temporal.workflow.Workflow} code.
 *
 * It also differs from {@link Closeable} because the close method does not throw any checked exceptions
 */
public interface EventTimer extends Closeable {
  void close();
}
