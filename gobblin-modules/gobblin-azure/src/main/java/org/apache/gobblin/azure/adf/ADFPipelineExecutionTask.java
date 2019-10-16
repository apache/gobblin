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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.keyvault.models.SecretBundle;
import joptsimple.internal.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.azure.aad.AADTokenRequesterImpl;
import org.apache.gobblin.azure.aad.CachedAADAuthenticator;
import org.apache.gobblin.azure.key_vault.KeyVaultSecretRetriever;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.workunit.WorkUnit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An example implementation of AbstractADFPipelineExecutionTask
 * - The pipeline parameters are passed using the configuration {@link ADFConfKeys#AZURE_DATA_FACTORY_PIPELINE_PARAM}
 * - The authentication token are fetched against AAD through either
 * 1. {@link ADFConfKeys#AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID} + {@link ADFConfKeys#AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_SECRET}
 * 2. {@link ADFConfKeys#AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID} + key vault settings like
 * -- --  {@link ADFConfKeys#AZURE_KEY_VAULT_URL}
 * -- --  {@link ADFConfKeys#AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID}
 * -- --  {@link ADFConfKeys#AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET}
 * -- --  {@link ADFConfKeys#AZURE_KEY_VAULT_SECRET_ADF_EXEC}
 */
@Slf4j
public class ADFPipelineExecutionTask extends AbstractADFPipelineExecutionTask {
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
    return jsonToMap(this.wu.getProp(ADFConfKeys.AZURE_DATA_FACTORY_PIPELINE_PARAM, ""));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AuthenticationResult getAuthenticationToken() {
    //First: get ADF executor credential
    //Check whether it's provided directly
    String spAdfExeSecret = this.wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_SECRET, "");
    if (spAdfExeSecret.isEmpty()) {
      //get ADF executor credential from key vault if not explicitly provided
      String spAkvReaderId = this.wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID);
      String spAkvReaderSecret = this.wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET);
      String keyVaultUrl = this.wu.getProp(ADFConfKeys.AZURE_KEY_VAULT_URL);
      String spAdfExeSecretName = this.wu.getProp(ADFConfKeys.AZURE_KEY_VAULT_SECRET_ADF_EXEC);

      SecretBundle fetchedSecret = new KeyVaultSecretRetriever(keyVaultUrl).getSecret(spAkvReaderId, spAkvReaderSecret, spAdfExeSecretName);
      spAdfExeSecret = fetchedSecret.value();
    }
    String aadId = this.wu.getProp(ADFConfKeys.AZURE_ACTIVE_DIRECTORY_ID);
    String spAdfExeId = this.wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID);

    //Second: get ADF executor token from AAD based on the id and secret
    AuthenticationResult token;
    try {
      CachedAADAuthenticator cachedAADAuthenticator = CachedAADAuthenticator.buildWithAADId(aadId);
      token = cachedAADAuthenticator.getToken(AADTokenRequesterImpl.TOKEN_TARGET_RESOURCE_MANAGEMENT, spAdfExeId, spAdfExeSecret);
    } catch (Exception e) {
      this.workingState = WorkUnitState.WorkingState.FAILED;
      throw new RuntimeException(e);
    }
    return token;
  }

  public static Map<String, String> jsonToMap(String json) {
    if (Strings.isNullOrEmpty(json)) {
      return new HashMap<>();
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, new TypeReference<HashMap<String, String>>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(String.format("Fail to convert the string %s to a map", json), e);
    }
  }
}
