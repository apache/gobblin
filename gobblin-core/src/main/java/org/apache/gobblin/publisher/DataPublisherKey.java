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

package org.apache.gobblin.publisher;

import org.apache.gobblin.broker.iface.SharedResourceKey;
import org.apache.gobblin.configuration.State;

import lombok.Getter;


/**
 * {@link SharedResourceKey} for requesting {@link DataPublisher}s from a
 * {@link org.apache.gobblin.broker.iface.SharedResourceFactory
 */
@Getter
public class DataPublisherKey implements SharedResourceKey {
  private final String publisherClassName;
  private final State state;

  public DataPublisherKey(String publisherClassName, State state) {
    this.publisherClassName = publisherClassName;
    this.state = state;
  }

  @Override
  public String toConfigurationKey() {
    return this.publisherClassName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataPublisherKey that = (DataPublisherKey) o;

    return publisherClassName == null ?
        that.publisherClassName == null : publisherClassName.equals(that.publisherClassName);
  }

  @Override
  public int hashCode() {
    return publisherClassName != null ? publisherClassName.hashCode() : 0;
  }
}
