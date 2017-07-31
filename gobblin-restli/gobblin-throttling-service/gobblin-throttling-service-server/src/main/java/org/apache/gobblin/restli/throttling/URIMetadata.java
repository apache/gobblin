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

package org.apache.gobblin.restli.throttling;

import java.net.URI;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A {@link org.apache.gobblin.restli.throttling.LeaderFinder.Metadata} that contains the {@link URI} of the process.
 */
@AllArgsConstructor
public class URIMetadata implements LeaderFinder.Metadata {
  @Getter
  private final URI uri;

  @Override
  public String getShortName() {
    return this.uri.toString().replaceAll("[:/]", "_");
  }
}
