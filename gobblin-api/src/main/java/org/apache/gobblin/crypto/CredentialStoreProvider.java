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
package gobblin.crypto;

import java.util.Map;


/**
 * Represents a factory object that can build CredentialStores based on a set of config.
 */
public interface CredentialStoreProvider {
  /**
   * Build a credential store based on the passed in configuration parameters -
   * EncryptionConfigParser can be used to help parse known keys out of the config bag.
   *
   * If this provider does not know how to build the requested credential store it should
   * return null.
   */
  CredentialStore buildCredentialStore(Map<String, Object> parameters);
}
