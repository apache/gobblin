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

package org.apache.gobblin.config.store.api;

import java.net.URI;

import org.apache.gobblin.annotation.Alpha;


@Alpha
/** Denotes that a requested config version is invalid or is no longer available. */
public class VersionDoesNotExistException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private static final String MESSAGE_FORMAT =
      "Failed to find the version %s in config store %s because of: %s ";

  private final URI storeURI;
  private final String configVersion;

  public VersionDoesNotExistException(URI storeURI, String configVersion, String errorMessage) {
    super(String.format(MESSAGE_FORMAT, configVersion, storeURI, errorMessage));
    this.storeURI = storeURI;
    this.configVersion = configVersion;
  }

  public VersionDoesNotExistException(URI storeURI, String configVersion, Exception e) {
    super(String.format(MESSAGE_FORMAT, configVersion, storeURI, e.getMessage()), e);
    this.storeURI = storeURI;
    this.configVersion = configVersion;
  }

  public URI getStoreURI() {
    return this.storeURI;
  }

  public String getConfigVersion() {
    return this.configVersion;
  }
}
