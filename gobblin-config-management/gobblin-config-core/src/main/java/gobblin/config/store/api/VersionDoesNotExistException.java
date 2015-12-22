/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.config.store.api;

import java.net.URI;


public class VersionDoesNotExistException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 2736458021800944664L;

  private final URI storeURI;
  private final String version;

  public VersionDoesNotExistException(URI storeURI, String version, String message) {
    super(String
        .format("failed to find the version %s in config store %s with message %s ", version, storeURI, message));
    this.storeURI = storeURI;
    this.version = version;
  }

  public VersionDoesNotExistException(URI storeURI, String version, Exception e) {
    super(e);
    this.storeURI = storeURI;
    this.version = version;
  }

  public URI getStoreURI() {
    return storeURI;
  }

  public String getVersion() {
    return version;
  }
}
