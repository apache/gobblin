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

import lombok.Data;

/**
 * A helper class that builds the Azure Data Factory HTTP endpoint URL as documented
 * in <a href="https://docs.microsoft.com/en-us/rest/api/datafactory/">
 * <p>
 * The full url template is like
 * https://management.azure.com/subscriptions/<subscription_id>/resourceGroups/<rg_name>/providers/Microsoft.DataFactory/
 * factories/<data_factory_name>/pipelines/<pipeline_name>/<action_name>?api-version=<api_version>
 */
@Data
public class ADFPipelineExecutionUriBuilder {
  public final static String URL_TEMPLATE_PREFIX =
      "https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers/Microsoft.DataFactory/factories/%s";
  public final static String URL_TEMPLATE_SUFFIX = "?api-version=%s";
  /**
   * the id of the subscription within which your data factory is created
   */
  private final String subscriptionId;
  /**
   * the name of the resource group within which your data factory is created
   */
  private final String resourceGroupName;
  /**
   * the data factory name
   */
  private final String dataFactoryName;
  /**
   * the api version of the ADF rest endpoint
   */
  private final String apiVersion;


  /**
   * @return the resolved URL prefix up to data factory level
   */
  public String getResolvedPrefix() {
    return String.format(URL_TEMPLATE_PREFIX, subscriptionId, resourceGroupName, dataFactoryName);
  }

  /**
   * @return the resolved URL suffix for the api version
   */
  public String getResolvedSuffix() {
    return String.format(URL_TEMPLATE_SUFFIX, apiVersion);
  }

  /**
   * @param pipelineName the name of a pipeline you want to execute
   */
  public String buildPipelineRunUri(String pipelineName) {
    return buildPipelineActionUrl(pipelineName, "createRun");
  }

  /**
   * @param pipelineName the name of a pipeline you want to perform an action against
   * @param actionName   the action name
   * @return the fully constructed URL
   */
  public String buildPipelineActionUrl(String pipelineName, String actionName) {
    return String.format("%s/pipelines/%s/%s%s", getResolvedPrefix(), pipelineName, actionName, getResolvedSuffix());
  }
}
