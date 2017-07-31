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

package org.apache.gobblin.restli;

import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Preconditions;

import lombok.Getter;


/**
 * A {@link SharedRestClientKey} that explicitly specifies the {@link URI} of the remote server.
 */
public class UriRestClientKey extends SharedRestClientKey {
  @Getter
  private final String uri;

  public UriRestClientKey(String serviceName, URI uri) {
    super(serviceName);
    try {
      Preconditions.checkNotNull(uri, "URI cannot be null.");
      this.uri = SharedRestClientFactory.resolveUriPrefix(uri);
    } catch (URISyntaxException use) {
      // THis should never happen
      throw new RuntimeException(use);
    }
  }

  /**
   * This constructor assumes uriPrefix is correctly formatted. Most use cases should use the constructor
   * {@link UriRestClientKey(String, URI)} instead.
   */
  public UriRestClientKey(String serviceName, String uriPrefix) {
    super(serviceName);
    this.uri = uriPrefix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    UriRestClientKey that = (UriRestClientKey) o;

    return uri.equals(that.uri);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + uri.hashCode();
    return result;
  }
}
