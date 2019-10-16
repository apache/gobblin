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

package org.apache.gobblin.azure.adf;

public class ADFConfKeys {
  public static final String AZURE_CONF_PREFIX = "azure.";
  public static final String AZURE_SUBSCRIPTION_ID = AZURE_CONF_PREFIX + "subscription.id";
  public static final String AZURE_ACTIVE_DIRECTORY_ID = AZURE_CONF_PREFIX + "aad.id";
  public static final String AZURE_RESOURCE_GROUP_NAME = AZURE_CONF_PREFIX + "resource-group.name";

  // service principal configuration
  public static final String AZURE_SERVICE_PRINCIPAL_PREFIX = AZURE_CONF_PREFIX + "service-principal.";
  public static final String AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID = AZURE_SERVICE_PRINCIPAL_PREFIX + "adf-executor.id";
  public static final String AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_SECRET = AZURE_SERVICE_PRINCIPAL_PREFIX + "adf-executor.secret";
  public static final String AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID = AZURE_SERVICE_PRINCIPAL_PREFIX + "key-vault-reader.id";
  public static final String AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET = AZURE_SERVICE_PRINCIPAL_PREFIX + "key-vault-reader.secret";

  // data factory configuration
  public static final String AZURE_DATA_FACTORY_PREFIX = AZURE_CONF_PREFIX + "data-factory.";
  public static final String AZURE_DATA_FACTORY_NAME = AZURE_DATA_FACTORY_PREFIX + "name";
  public static final String AZURE_DATA_FACTORY_PIPELINE_NAME = AZURE_DATA_FACTORY_PREFIX + "pipeline.name";
  public static final String AZURE_DATA_FACTORY_PIPELINE_PARAM = AZURE_DATA_FACTORY_PREFIX + "pipeline.params";
  public static final String AZURE_DATA_FACTORY_API_VERSION = AZURE_DATA_FACTORY_PREFIX + "api.version";

  // key vault configuration
  public static final String AZURE_KEY_VAULT_PREFIX = AZURE_CONF_PREFIX + "key-vault.";
  public static final String AZURE_KEY_VAULT_URL = AZURE_KEY_VAULT_PREFIX + "url";
  public static final String AZURE_KEY_VAULT_SECRET_PREFIX = AZURE_KEY_VAULT_PREFIX + "secret-name.";
  public static final String AZURE_KEY_VAULT_SECRET_ADF_EXEC = AZURE_KEY_VAULT_SECRET_PREFIX + "adf-executor";
}
