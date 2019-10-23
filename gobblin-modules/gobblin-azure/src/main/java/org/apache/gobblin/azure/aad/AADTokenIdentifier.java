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

package org.apache.gobblin.azure.aad;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * The key that uniquely identifies a token
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class AADTokenIdentifier {
  //an azure active directory authority url of the pattern "https://login.microsoftonline.com/<aad_id>"
  private final String authorityUrl;
  //identifier of the target resource that is the recipient of the requested token
  private final String targetResource;
  //the service principal id
  private final String servicePrincipalId;
  //the service principal secret
  private final String servicePrincipalSecret;

  @Override
  public String toString() {
    /**
     * Avoid printing the service principal's secret in the log
     */
    return String.format("%s{authorityUrl='%s', targetResource='%s', servicePrincipalId='%s'}",
        AADTokenIdentifier.class.getSimpleName(), authorityUrl, targetResource, servicePrincipalId);
  }
}
