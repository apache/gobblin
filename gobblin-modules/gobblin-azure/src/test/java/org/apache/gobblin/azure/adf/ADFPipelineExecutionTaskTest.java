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

import com.microsoft.azure.keyvault.models.SecretBundle;
import org.apache.gobblin.azure.aad.AADTokenRequesterImpl;
import org.apache.gobblin.azure.aad.CachedAADAuthenticator;
import org.apache.gobblin.azure.aad.CachedAADAuthenticatorTest;
import org.apache.gobblin.azure.key_vault.KeyVaultSecretRetriever;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.util.TaskMetrics;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.task.HttpExecutionTask;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * A test class uses PowerMockito and Testng
 * References:
 * https://github.com/powermock/powermock/issues/434
 * https://jivimberg.io/blog/2016/04/03/using-powermock-plus-testng-to-mock-a-static-class/
 */
@Test
@PrepareForTest({TaskMetrics.class, KeyVaultSecretRetriever.class, CachedAADAuthenticator.class})
public class ADFPipelineExecutionTaskTest extends PowerMockTestCase {
  @Test
  public void testBuildUri() throws Exception {
    WorkUnit wu = WorkUnit.createEmpty();
    wu.setProp(ADFConfKeys.AZURE_SUBSCRIPTION_ID, "123");
    String aadId = "456";
    wu.setProp(ADFConfKeys.AZURE_ACTIVE_DIRECTORY_ID, aadId);
    wu.setProp(ADFConfKeys.AZURE_RESOURCE_GROUP_NAME, "rg1");
    wu.setProp(ADFConfKeys.AZURE_DATA_FACTORY_NAME, "df1");
    wu.setProp(ADFConfKeys.AZURE_DATA_FACTORY_PIPELINE_NAME, "p1");
    wu.setProp(ADFConfKeys.AZURE_DATA_FACTORY_API_VERSION, "v1");
    String akvUrl = "akv-url";
    wu.setProp(ADFConfKeys.AZURE_KEY_VAULT_URL, akvUrl);
    String akvExecId = "akv_exec_id";
    wu.setProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID, akvExecId);
    wu.setProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID, "akv_reader_id");
    wu.setProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET, "akv_reader_secret");
    wu.setProp(ADFConfKeys.AZURE_KEY_VAULT_SECRET_ADF_EXEC, "adf_exec_secret_name");
    wu.setProp(ADFConfKeys.AZURE_DATA_FACTORY_PIPELINE_PARAM, "{\"date\":\"2019-10-15\",\"customerName\":\"chen\",\"password\":\"qwert12345!\",\"securityToken\":\"12345\"}");
    wu.setProp(HttpExecutionTask.CONF_HTTPTASK_TYPE, HttpExecutionTask.HttpMethod.POST);

    WorkUnitState wuState = new WorkUnitState(wu, new State());
    TaskState taskState = new TaskState(wuState);
    taskState.setJobId("job_123");
    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getTaskState()).thenReturn(taskState);

    PowerMockito.mockStatic(TaskMetrics.class);

    TaskMetrics taskMetrics = mock(TaskMetrics.class);
    when(taskMetrics.getMetricContext()).thenReturn(null);
    when(TaskMetrics.get(taskState)).thenReturn(taskMetrics);

    ADFPipelineExecutionTask task = new ADFPipelineExecutionTask(taskContext);

    String fetchedAdfExecToken = "my_token";
    PowerMockito.stub(PowerMockito.method(KeyVaultSecretRetriever.class, "getSecret",
        String.class, String.class, String.class)).toReturn(new SecretBundle().withValue(fetchedAdfExecToken));


    CachedAADAuthenticator cachedAADAuthenticator = mock(CachedAADAuthenticator.class);
    when(cachedAADAuthenticator.getToken(AADTokenRequesterImpl.TOKEN_TARGET_RESOURCE_MANAGEMENT, akvExecId, fetchedAdfExecToken))
        .thenReturn(CachedAADAuthenticatorTest.url1s1sp1token2);
    PowerMockito.mockStatic(CachedAADAuthenticator.class);

    PowerMockito.stub(PowerMockito.method(CachedAADAuthenticator.class, "getToken",
        String.class, String.class, String.class)).toReturn(CachedAADAuthenticatorTest.url1s1sp1token2);

    when(CachedAADAuthenticator.buildWithAADId(aadId)).thenReturn(cachedAADAuthenticator);
    HttpUriRequest httpRequest = task.createHttpUriRequest();
    Assert.assertEquals(httpRequest.getURI().toString(), "https://management.azure.com/subscriptions/123/resourceGroups/rg1/providers/Microsoft.DataFactory/factories/df1/pipelines/p1/createRun?api-version=v1");
    Assert.assertEquals(httpRequest.getMethod(), "POST");

    Header[] allHeaders = httpRequest.getAllHeaders();
    Assert.assertEquals(allHeaders.length, 3);
    Assert.assertEquals(httpRequest.getHeaders("Content-Type")[0].getValue(), "application/json");
    Assert.assertEquals(httpRequest.getHeaders("Accept")[0].getValue(), "application/json");
    Assert.assertEquals(httpRequest.getHeaders("Authorization")[0].getValue(), "Bearer " + CachedAADAuthenticatorTest.url1s1sp1token2AccessToken);
    HttpEntity entity = ((HttpPost) httpRequest).getEntity();
    Map<String, String> map = ADFPipelineExecutionTask.jsonToMap(EntityUtils.toString(entity));
    Assert.assertEquals(map.size(), 4);
    Assert.assertEquals(map.get("date"), "2019-10-15");
    Assert.assertEquals(map.get("customerName"), "chen");
    Assert.assertEquals(map.get("password"), "qwert12345!");
    Assert.assertEquals(map.get("securityToken"), "12345");
  }

  @Test
  public void testJsonToMap1() {
    Map<String, String> map = ADFPipelineExecutionTask.jsonToMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
    Assert.assertEquals(map.size(), 2);
    Assert.assertEquals(map.get("k1"), "v1");
    Assert.assertEquals(map.get("k2"), "v2");
  }

  @Test
  public void testJsonToMap2() {
    Map<String, String> map = ADFPipelineExecutionTask.jsonToMap("");
    Assert.assertEquals(map.size(), 0);
  }

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }
}
