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
package org.apache.gobblin.admin;

import java.net.URI;
import java.util.Properties;

import com.google.common.util.concurrent.Service;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.runtime.api.AdminWebServerFactory;

/**
 * A factory for the default implementation of the admin web server
 */
@Alias(value="default")
public class DefaultAdminWebServerFactory implements AdminWebServerFactory {

  /** {@inheritDoc} */
  @Override
  public Service createInstance(Properties config, URI executionInfoServerURI) {
    return new AdminWebServer(config, executionInfoServerURI);
  }

}
