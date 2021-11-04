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

package org.apache.gobblin.service.modules.orchestration;

import com.google.common.io.Closer;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;

import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


/**
 * This class encapsulates all the operations an {@link AzkabanClient} can do.
 */
class AzkabanMultiCallables {

  /**
   * This class can never been instantiated.
   */
  private AzkabanMultiCallables() {
  }

  /**
   * A callable that will create a project on Azkaban.
   */
  @Builder
  static class CreateProjectCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String projectName;
    private String description;
    private boolean invalidSession = false;

    @Override
    public AzkabanClientStatus call()
        throws AzkabanClientException {

      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        HttpPost httpPost = new HttpPost(client.url + "/manager");
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.ACTION, "create"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.NAME, projectName));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.DESCRIPTION, description));
        httpPost.setEntity(new UrlEncodedFormEntity(nvps));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");
        httpPost.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpPost);
        closer.register(response);
        AzkabanClient.handleResponse(response);
        return new AzkabanSuccess();
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Throwable e) {
        throw new AzkabanClientException("Azkaban client cannot create project = "
            + projectName, e);
      }
    }
  }

  /**
   * A callable that will delete a project on Azkaban.
   */
  @Builder
  static class DeleteProjectCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String projectName;
    private boolean invalidSession = false;

    @Override
    public AzkabanClientStatus call()
        throws AzkabanClientException {

      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.DELETE, "true"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.PROJECT, projectName));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

        HttpGet httpGet = new HttpGet(client.url + "/manager?" + URLEncodedUtils.format(nvps, "UTF-8"));
        httpGet.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpGet);
        closer.register(response);
        AzkabanClient.verifyStatusCode(response);

        return new AzkabanSuccess();
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Throwable e) {
        throw new AzkabanClientException("Azkaban client cannot delete project = "
            + projectName, e);
      }
    }
  }

  /**
   * A callable that will execute a flow on Azkaban.
   */
  @Builder
  static class UploadProjectCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String projectName;
    private File zipFile;
    private boolean invalidSession = false;

    @Override
    public AzkabanClientStatus call()
        throws AzkabanClientException {

      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        HttpPost httpPost = new HttpPost(client.url + "/manager");
        HttpEntity entity = MultipartEntityBuilder.create()
            .addTextBody(AzkabanClientParams.SESSION_ID, client.sessionId)
            .addTextBody(AzkabanClientParams.AJAX, "upload")
            .addTextBody(AzkabanClientParams.PROJECT, projectName)
            .addBinaryBody("file", zipFile,
                ContentType.create("application/zip"), zipFile.getName())
            .build();
        httpPost.setEntity(entity);

        CloseableHttpResponse response = client.httpClient.execute(httpPost);
        closer.register(response);
        AzkabanClient.handleResponse(response);
        return new AzkabanSuccess();
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Throwable e) {
        throw new AzkabanClientException("Azkaban client cannot upload zip to project = "
            + projectName, e);
      }
    }
  }

  /**
   * A callable that will execute a flow on Azkaban.
   */
  @Builder
  static class ExecuteFlowCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String projectName;
    private String flowName;
    private Map<String, String> flowOptions;
    private Map<String, String> flowParameters;
    private boolean invalidSession = false;

    @Override
    public AzkabanExecuteFlowStatus call()
        throws AzkabanClientException {
      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        HttpPost httpPost = new HttpPost(client.url + "/executor");
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "executeFlow"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.PROJECT, projectName));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.FLOW, flowName));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.CONCURRENT_OPTION, "ignore"));

        addFlowOptions(nvps, flowOptions);
        addFlowParameters(nvps, flowParameters);

        httpPost.setEntity(new UrlEncodedFormEntity(nvps));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");
        httpPost.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpPost);
        closer.register(response);
        Map<String, String> map = AzkabanClient.handleResponse(response);
        return new AzkabanExecuteFlowStatus(
            new AzkabanExecuteFlowStatus.ExecuteId(map.get(AzkabanClientParams.EXECID)));
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Exception e) {
        throw new AzkabanClientException("Azkaban client cannot execute flow = "
            + flowName, e);
      }
    }

    private void addFlowParameters(List<NameValuePair> nvps, Map<String, String> flowParams) {
      if (flowParams != null) {
        for (Map.Entry<String, String> entry : flowParams.entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue();
          if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
            nvps.add(new BasicNameValuePair("flowOverride[" + key + "]", value));
          }
        }
      }
    }

    private void addFlowOptions(List<NameValuePair> nvps, Map<String, String> flowOptions) {
      if (flowOptions != null) {
        for (Map.Entry<String, String> option : flowOptions.entrySet()) {
          nvps.add(new BasicNameValuePair(option.getKey(), option.getValue()));
        }
      }
    }
  }

  /**
   * A callable that will cancel a flow on Azkaban.
   */
  @Builder
  static class CancelFlowCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String execId;
    private boolean invalidSession = false;

    @Override
    public AzkabanClientStatus call()
        throws AzkabanClientException {
      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "cancelFlow"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.EXECID, String.valueOf(execId)));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

        HttpGet httpGet = new HttpGet(client.url + "/executor?" + URLEncodedUtils.format(nvps, "UTF-8"));
        httpGet.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpGet);
        closer.register(response);
        AzkabanClient.handleResponse(response);
        return new AzkabanSuccess();
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Exception e) {
        throw new AzkabanClientException("Azkaban client cannot cancel flow execId = "
            + execId, e);
      }
    }
  }

  /**
   * A callable that will fetch a flow status on Azkaban.
   */
  @Builder
  static class FetchFlowExecCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String execId;
    private boolean invalidSession = false;

    @Override
    public AzkabanFetchExecuteFlowStatus call()
        throws AzkabanClientException {
      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "fetchexecflow"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.EXECID, execId));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

        HttpGet httpGet = new HttpGet(client.url + "/executor?" + URLEncodedUtils.format(nvps, "UTF-8"));
        httpGet.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpGet);
        closer.register(response);
        Map<String, String> map = AzkabanClient.handleResponse(response);
        return new AzkabanFetchExecuteFlowStatus(new AzkabanFetchExecuteFlowStatus.Execution(map));
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Exception e) {
        throw new AzkabanClientException("Azkaban client cannot "
            + "fetch execId " + execId, e);
      }
    }
  }


  /**
   * A callable that will fetch a flow status on Azkaban.
   */
  @Builder
  static class FetchProjectFlowsCallable implements Callable<AzkabanProjectFlowsStatus> {
    private AzkabanClient client;
    private boolean invalidSession = false;
    private String projectName;

    @Override
    public AzkabanProjectFlowsStatus call()
            throws AzkabanClientException {
      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "fetchprojectflows"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.PROJECT, projectName));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

        HttpGet httpGet = new HttpGet(client.url + "/manager?" + URLEncodedUtils.format(nvps, "UTF-8"));
        httpGet.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpGet);
        closer.register(response);

        AzkabanProjectFlowsStatus.Project project =
                AzkabanClient.handleResponse(response, AzkabanProjectFlowsStatus.Project.class);
        return new AzkabanProjectFlowsStatus(project);
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Exception e) {
        throw new AzkabanClientException("Azkaban client cannot fetch project flows", e);
      }
    }
  }

  /**
   * A callable that will fetch a flow log on Azkaban.
   */
  @Builder
  static class FetchExecLogCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String execId;
    private String jobId;
    private long offset;
    private long length;
    private OutputStream output;
    private boolean invalidSession = false;

    @Override
    public AzkabanClientStatus call()
        throws AzkabanClientException {
      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "fetchExecJobLogs"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.EXECID, execId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.JOBID, jobId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.OFFSET, String.valueOf(offset)));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.LENGTH, String.valueOf(length)));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

        HttpGet httpGet = new HttpGet(client.url + "/executor?" + URLEncodedUtils.format(nvps, "UTF-8"));
        httpGet.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpGet);
        closer.register(response);
        Map<String, String> map = AzkabanClient.handleResponse(response);

        try (Writer logWriter = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
          logWriter.write(map.get(AzkabanClientParams.DATA));
        }

        return new AzkabanSuccess();
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Exception e) {
        throw new AzkabanClientException("Azkaban client cannot "
            + "fetch execId " + execId, e);
      }
    }
  }

  /**
   * A callable that will add a proxy user to a project on Azkaban
   */
  @Builder
  static class AddProxyUserCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String projectName;
    private String proxyUserName;
    private boolean invalidSession = false;

    @Override
    public AzkabanClientStatus call()
        throws AzkabanClientException {
      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "addProxyUser"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.PROJECT, projectName));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.NAME, proxyUserName));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

        HttpGet httpGet = new HttpGet(client.url + "/manager?" + URLEncodedUtils.format(nvps, "UTF-8"));
        httpGet.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpGet);
        closer.register(response);
        AzkabanClient.handleResponse(response);
        return new AzkabanSuccess();
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Exception e) {
        throw new AzkabanClientException("Azkaban client cannot add proxy user " + proxyUserName, e);
      }
    }
  }

  /**
   * A callable that will get the list of proxy users from a project on Azkaban.
   */
  @Builder
  static class GetProxyUserCallable implements Callable<AzkabanClientStatus> {
    private AzkabanClient client;
    private String projectName;
    private boolean invalidSession = false;

    @Override
    public AzkabanClientStatus call()
        throws AzkabanClientException {
      try (Closer closer = Closer.create()) {
        client.refreshSession(this.invalidSession);
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "getProxyUsers"));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, client.sessionId));
        nvps.add(new BasicNameValuePair(AzkabanClientParams.PROJECT, projectName));

        Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

        HttpGet httpGet = new HttpGet(client.url + "/manager?" + URLEncodedUtils.format(nvps, "UTF-8"));
        httpGet.setHeaders(new Header[]{contentType, requestType});

        CloseableHttpResponse response = client.httpClient.execute(httpGet);
        closer.register(response);
        Map<String, String> map = AzkabanClient.handleResponse(response);
        return new AzkabanGetProxyUsersStatus(new AzkabanGetProxyUsersStatus.ProxyUsers(map));
      } catch (InvalidSessionException e) {
        this.invalidSession = true;
        throw e;
      } catch (Exception e) {
        throw new AzkabanClientException(String.format("Azkaban client failed to get proxy users for %s", client.url), e);
      }
    }
  }
}
