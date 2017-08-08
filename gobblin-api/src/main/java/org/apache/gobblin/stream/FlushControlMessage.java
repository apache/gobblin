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
package org.apache.gobblin.stream;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * Control message for flushing writers
 * @param <D>
 */
@AllArgsConstructor
@EqualsAndHashCode
public class FlushControlMessage<D> extends ControlMessage<D> {
  @Getter
  private final FlushReason flushReason;

  @Override
  protected StreamEntity<D> buildClone() {
    return new FlushControlMessage(flushReason);
  }

  @AllArgsConstructor
  @EqualsAndHashCode
  public static class FlushReason {
    @Getter
    private final String reason;
  }
}