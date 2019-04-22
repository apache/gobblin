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

import java.net.URI;

/**
 * An exception when {@link Spec} cannot be correctly serialized/deserialized from underlying storage.
 */
public class SpecSerDeException extends Exception{
  private static final long serialVersionUID = 1L;

  /**
   * The URI that triggered SerDe error.
   * Could be a single Spec's URI or parent-level URI.
   */
  private final URI errorUri;

  public SpecSerDeException(URI errorUri) {
    super("Error occurred in loading of Spec with URI " + errorUri );
    this.errorUri = errorUri;
  }

  public SpecSerDeException(URI errorUri, Throwable cause) {
    super("Error occurred in loading URI " + errorUri, cause);
    this.errorUri = errorUri;
  }

  public SpecSerDeException(String errorMsg, URI errorUri, Throwable cause) {
    super("Error occurred in loading URI " + errorUri + " with message:" + errorMsg, cause);
    this.errorUri = errorUri;
  }

  public URI getErrorUri() {
    return errorUri;
  }
}
