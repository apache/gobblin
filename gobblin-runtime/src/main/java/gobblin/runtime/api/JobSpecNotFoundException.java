/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.api;

import java.net.URI;

/**
 * An exception thrown when a JobSpec with a given URI is not found.
 */
public class JobSpecNotFoundException extends Exception {
  private static final long serialVersionUID = 1L;
  private final URI _missingJobSpecURI;

  public JobSpecNotFoundException(URI missingJobSpecURI) {
    super("No JobSpec with URI " + missingJobSpecURI);
    _missingJobSpecURI = missingJobSpecURI;
  }

  public URI getMissingJobSpecURI() {
    return _missingJobSpecURI;
  }
}
