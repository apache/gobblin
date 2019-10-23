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

import java.util.Map;

import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.keyvault.models.SecretBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.azure.aad.AADTokenRequesterImpl;
import org.apache.gobblin.azure.aad.CachedAADAuthenticator;
import org.apache.gobblin.azure.key_vault.KeyVaultSecretRetriever;
import org.apache.gobblin.azure.util.JsonUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * An example implementation of AbstractADFPipelineExecutionTask
 * - The pipeline parameters are passed using the configuration {@link #AZURE_DATA_FACTORY_PIPELINE_PARAM}
 * - The authentication token are fetched against AAD through either
 * 1. {@link #AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID} + {@link #AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_SECRET}
 * 2. {@link #AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID} + key vault settings like
 * -- --  {@link #AZURE_KEY_VAULT_URL}
 * -- --  {@link #AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID}
 * -- --  {@link #AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET}
 * -- --  {@link #AZURE_KEY_VAULT_SECRET_ADF_EXEC}
 */
@Slf4j
public class ADFPipelineExecutionTask extends AbstractADFPipelineExecutionTask {
  public static final String AZURE_CONF_PREFIX = "azure.";
  static final String AZURE_SUBSCRIPTION_ID = AZURE_CONF_PREFIX + "subscription.id";
  static final String AZURE_ACTIVE_DIRECTORY_ID = AZURE_CONF_PREFIX + "aad.id";
  static final String AZURE_RESOURCE_GROUP_NAME = AZURE_CONF_PREFIX + "resource-group.name";

  // service principal configuration
  static final String AZURE_SERVICE_PRINCIPAL_PREFIX = AZURE_CONF_PREFIX + "service-principal.";
  static final String AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID = AZURE_SERVICE_PRINCIPAL_PREFIX + "adf-executor.id";
  static final String AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_SECRET =
      AZURE_SERVICE_PRINCIPAL_PREFIX + "adf-executor.secret";
  static final String AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID =
      AZURE_SERVICE_PRINCIPAL_PREFIX + "key-vault-reader.id";
  static final String AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET =
      AZURE_SERVICE_PRINCIPAL_PREFIX + "key-vault-reader.secret";

  // data factory configuration
  static final String AZURE_DATA_FACTORY_PREFIX = AZURE_CONF_PREFIX + "data-factory.";
  static final String AZURE_DATA_FACTORY_NAME = AZURE_DATA_FACTORY_PREFIX + "name";
  static final String AZURE_DATA_FACTORY_PIPELINE_NAME = AZURE_DATA_FACTORY_PREFIX + "pipeline.name";
  static final String AZURE_DATA_FACTORY_PIPELINE_PARAM = AZURE_DATA_FACTORY_PREFIX + "pipeline.params";
  static final String AZURE_DATA_FACTORY_API_VERSION = AZURE_DATA_FACTORY_PREFIX + "api.version";

  // key vault configuration
  static final String AZURE_KEY_VAULT_PREFIX = AZURE_CONF_PREFIX + "key-vault.";
  static final String AZURE_KEY_VAULT_URL = AZURE_KEY_VAULT_PREFIX + "url";
  static final String AZURE_KEY_VAULT_SECRET_PREFIX = AZURE_KEY_VAULT_PREFIX + "secret-name.";
  static final String AZURE_KEY_VAULT_SECRET_ADF_EXEC = AZURE_KEY_VAULT_SECRET_PREFIX + "adf-executor";

  private final WorkUnit wu;

  public ADFPipelineExecutionTask(TaskContext taskContext) {
    super(taskContext);
    TaskState taskState = this.taskContext.getTaskState();
    wu = taskState.getWorkunit();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Map<String, String> providePayloads() {
    return JsonUtils.jsonToMap(this.wu.getProp(AZURE_DATA_FACTORY_PIPELINE_PARAM, ""));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AuthenticationResult getAuthenticationToken() {
    //First: get ADF executor credential
    //Check whether it's provided directly
    String spAdfExeSecret = this.wu.getProp(AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_SECRET, "");
    String aadId = this.wu.getProp(AZURE_ACTIVE_DIRECTORY_ID);
    CachedAADAuthenticator cachedAADAuthenticator = CachedAADAuthenticator.buildWithAADId(aadId, wu.getProperties());

    if (spAdfExeSecret.isEmpty()) {
      //get ADF executor credential from key vault if not explicitly provided
      String spAkvReaderId = this.wu.getProp(AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID);
      String spAkvReaderSecret = this.wu.getProp(AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET);
      String keyVaultUrl = this.wu.getProp(AZURE_KEY_VAULT_URL);
      String spAdfExeSecretName = this.wu.getProp(AZURE_KEY_VAULT_SECRET_ADF_EXEC);

      SecretBundle fetchedSecret = new KeyVaultSecretRetriever(keyVaultUrl, cachedAADAuthenticator)
          .getSecret(spAkvReaderId, spAkvReaderSecret, spAdfExeSecretName);
      spAdfExeSecret = fetchedSecret.value();
    }
    String spAdfExeId = this.wu.getProp(AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID);

    //Second: get ADF executor token from AAD based on the id and secret
    AuthenticationResult token;
    try {
      token = cachedAADAuthenticator
          .getToken(AADTokenRequesterImpl.TOKEN_TARGET_RESOURCE_MANAGEMENT, spAdfExeId, spAdfExeSecret);
    } catch (Exception e) {
      this.workingState = WorkUnitState.WorkingState.FAILED;
      throw new RuntimeException(e);
    }

    if (token == null) {
      throw new RuntimeException(
          String.format("Cannot fetch authentication token for SP %s against AAD %s", spAdfExeId, aadId));
    }
    return token;
  }
}
