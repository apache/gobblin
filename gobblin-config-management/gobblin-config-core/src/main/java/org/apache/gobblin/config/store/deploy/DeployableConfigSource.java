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
package org.apache.gobblin.config.store.deploy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.apache.gobblin.config.store.api.ConfigStore;


/**
 * An abstraction for accessing configs to be deployed by the {@link ConfigStore}. The interface finds all the configs
 * that need to be deployed and also provides a way to read them as {@link InputStream}s
 */
public interface DeployableConfigSource {

  /**
   * Open an {@link InputStream} for every config to be deployed.
   *
   * @return a {@link Set} of {@link ConfigStream}s for each resource to be deployed
   */
  public Set<ConfigStream> getConfigStreams() throws IOException;
}
