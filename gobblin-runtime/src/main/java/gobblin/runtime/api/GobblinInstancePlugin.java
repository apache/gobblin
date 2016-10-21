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
package gobblin.runtime.api;

import com.google.common.util.concurrent.Service;

/**
 * The interfaces for Gobblin instance plugins. Plugins are objects the share configuration and
 * lifespan with a specific Gobblin instance and can be used to extend its functionality. Examples
 * of plugins maybe providing a REST interface, authentication and so on.
 */
public interface GobblinInstancePlugin extends Service {

  /** Override this method to provide a human-readable description of the plugin */
  @Override String toString();

}
