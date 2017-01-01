/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase.writer;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.typesafe.config.Config;


/**
 * A factory to hand out {@link com.couchbase.client.java.env.CouchbaseEnvironment} instances
 */
public class CouchbaseEnvironmentFactory {

  private static CouchbaseEnvironment couchbaseEnvironment = null;

  /**
   * Currently hands out a singleton DefaultCouchbaseEnvironment.
   * This is done because it is recommended to use a single couchbase environment instance per JVM.
   * TODO: Determine if we need to use the config to tweak certain parameters
   * @param config
   * @return
   */
  public static synchronized CouchbaseEnvironment getInstance(Config config)
  {
    if (couchbaseEnvironment == null)
    {
      return DefaultCouchbaseEnvironment.create();
    }
    else {
      return couchbaseEnvironment;
    }
  }
}
