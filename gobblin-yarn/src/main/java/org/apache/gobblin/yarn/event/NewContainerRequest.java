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

package org.apache.gobblin.yarn.event;

import org.apache.hadoop.yarn.api.records.Container;

import com.google.common.base.Optional;


/**
 * A type of events for new container requests to be used with a {@link com.google.common.eventbus.EventBus}.
 *
 * @author Yinan Li
 */
public class NewContainerRequest {

  private final Optional<Container> replacedContainer;

  public NewContainerRequest(Optional<Container> replacedContainer) {
    this.replacedContainer = replacedContainer;
  }

  /**
   * Get (optionally) the {@link Container} to be replaced by the to be requested new container.
   *
   * @return an {@link Optional} of the {@link Container} to be replaced
   */
  public Optional<Container> getReplacedContainer() {
    return this.replacedContainer;
  }
}
